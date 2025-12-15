import time
import asyncio
import httpx
import logging
from datetime import datetime
from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, Boolean, DateTime, Text

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== КОНСТАНТЫ ==========
HN_URL = "https://hacker-news.firebaseio.com/v0/"
BACKGROUND_TASK_INTERVAL = 600  # 5 минут


# ========== АСИНХРОННАЯ БАЗА ДАННЫХ ==========
class Base(DeclarativeBase):
    pass


# Асинхронный движок SQLite
engine = create_async_engine(
    "sqlite+aiosqlite:///./todo.db",
    echo=False,
    future=True
)

# Асинхронная сессия
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)


class TaskModel(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    title: Mapped[str] = mapped_column(String(500))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    done: Mapped[bool] = mapped_column(Boolean, default=False)
    category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    priority: Mapped[int] = mapped_column(Integer, default=1)  # 1-5
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    source: Mapped[str] = mapped_column(String(50), default="manual")  # "manual" или "hacker_news"


# ========== МЕНЕДЖЕР ПОДКЛЮЧЕНИЙ WebSocket ==========
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Новое WebSocket подключение. Всего подключений: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket отключен. Осталось подключений: {len(self.active_connections)}")

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return

        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Ошибка отправки WebSocket: {e}")
                disconnected.append(connection)

        for connection in disconnected:
            self.disconnect(connection)


manager = ConnectionManager()

# ========== ФОНОВАЯ ЗАДАЧА ==========
background_task_running = False


async def fetch_top_stories():
    """Получение топовых новостей с Hacker News"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Получаем список ID топовых новостей
            response = await client.get(f"{HN_URL}topstories.json")
            response.raise_for_status()
            story_ids = response.json()[:10]  # Берем первые 10

            stories = []
            for story_id in story_ids:
                try:
                    # Получаем детали каждой новости
                    story_response = await client.get(f"{HN_URL}item/{story_id}.json")
                    if story_response.status_code == 200:
                        story_data = story_response.json()
                        if story_data and story_data.get("type") == "story":
                            stories.append(story_data)

                    # Небольшая задержка, чтобы не перегружать API
                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"Ошибка при получении новости {story_id}: {e}")
                    continue

            return stories

    except Exception as e:
        logger.error(f"Ошибка при получении топовых новостей: {e}")
        return []


async def save_hn_stories_to_db(stories: List[dict], db: AsyncSession):
    """Сохранение новостей Hacker News в базу как задачи"""
    added_count = 0

    for story in stories:
        try:
            title = story.get("title", "")
            url = story.get("url", "")
            text = story.get("text", "")

            if not title:
                continue

            # Формируем описание задачи
            description_parts = []
            if url:
                description_parts.append(f"URL: {url}")
            if text:
                description_parts.append(f"Text: {text[:200]}...")
            if story.get("score"):
                description_parts.append(f"Score: {story['score']}")
            if story.get("by"):
                description_parts.append(f"Author: {story['by']}")

            description = "\n".join(description_parts) if description_parts else "Новость с Hacker News"

            # Проверяем, существует ли уже такая задача
            stmt = select(TaskModel).where(
                TaskModel.title == title[:200],
                TaskModel.source == "hacker_news"
            )
            result = await db.execute(stmt)
            existing_task = result.scalar_one_or_none()

            if not existing_task:
                # Определяем приоритет на основе рейтинга
                score = story.get("score", 0)
                priority = 5 if score > 100 else (4 if score > 50 else (3 if score > 20 else (2 if score > 10 else 1)))

                new_task = TaskModel(
                    title=title[:200],
                    description=description[:1000],
                    category="hacker_news",
                    priority=priority,
                    done=False,
                    source="hacker_news"
                )
                db.add(new_task)
                added_count += 1

                logger.info(f"Добавлена задача из HN: {title[:50]}...")

        except Exception as e:
            logger.error(f"Ошибка при обработке новости: {e}")
            continue

    await db.commit()

    if added_count > 0:
        # Отправляем уведомление через WebSocket
        await manager.broadcast({
            "type": "background_task_completed",
            "message": f"Добавлено {added_count} новостей из Hacker News",
            "timestamp": datetime.utcnow().isoformat(),
            "count": added_count
        })

    return added_count


async def background_task_worker():
    """Функция фоновой задачи"""
    global background_task_running

    while background_task_running:
        try:
            logger.info(f"Запуск фоновой задачи в {datetime.now().strftime('%H:%M:%S')}")

            # Получаем новости с Hacker News
            stories = await fetch_top_stories()

            if stories:
                # Сохраняем в базу данных
                async with AsyncSessionLocal() as db:
                    added_count = await save_hn_stories_to_db(stories, db)
                    logger.info(f"Успешно сохранено {added_count} новостей из Hacker News")

            # Ждем перед следующим запуском
            logger.info(f"Следующий запуск фоновой задачи через {BACKGROUND_TASK_INTERVAL} секунд")
            await asyncio.sleep(BACKGROUND_TASK_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Фоновая задача остановлена")
            break
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче: {e}")
            await asyncio.sleep(60)  # Ждем минуту при ошибке


# ========== LIFESPAN МЕНЕДЖЕР ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Создаем таблицы при запуске
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Запускаем фоновую задачу
    global background_task_running
    background_task_running = True
    task = asyncio.create_task(background_task_worker())

    logger.info("Приложение запущено")

    yield

    # Останавливаем фоновую задачу
    logger.info("Остановка приложения...")
    background_task_running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Закрываем соединения с БД
    await engine.dispose()
    logger.info("Приложение остановлено")


# ========== FASTAPI APP ==========
app = FastAPI(
    title="ToDo API with Hacker News Parser",
    description="REST API для управления задачами с WebSocket уведомлениями и фоновым парсингом Hacker News",
    version="1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


# ========== MIDDLEWARE ==========
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time

    logger.info(f"{request.method} {request.url.path} - {response.status_code} - {process_time:.4f}s")

    return response


# ========== ЗАВИСИМОСТИ ==========
async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ========== PYDANTIC МОДЕЛИ ==========
class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    category: Optional[str] = None
    priority: Optional[int] = 1


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    done: Optional[bool] = None
    category: Optional[str] = None
    priority: Optional[int] = None


class TaskResponse(BaseModel):
    id: int
    title: str
    description: Optional[str]
    done: bool
    category: Optional[str]
    priority: int
    created_at: datetime
    updated_at: datetime
    source: str

    class Config:
        from_attributes = True


class BackgroundTaskResponse(BaseModel):
    message: str
    task_count: int
    status: str


class WebSocketMessage(BaseModel):
    type: str
    message: str
    timestamp: datetime
    data: Optional[dict] = None


# ========== WebSocket ЭНДПОИНТ ==========
@app.websocket("/ws/tasks")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    # Отправляем приветственное сообщение
    await manager.send_personal_message({
        "type": "welcome",
        "message": "Подключение к WebSocket установлено",
        "timestamp": datetime.utcnow().isoformat(),
        "connections": len(manager.active_connections)
    }, websocket)

    try:
        while True:
            # Ожидаем сообщения от клиента
            data = await websocket.receive_text()

            # Обрабатываем сообщение от клиента
            try:
                await manager.send_personal_message({
                    "type": "echo",
                    "message": f"Получено: {data}",
                    "timestamp": datetime.utcnow().isoformat()
                }, websocket)
            except Exception as e:
                logger.error(f"Ошибка обработки WebSocket сообщения: {e}")

    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ========== REST API ЭНДПОИНТЫ ==========

@app.get("/", tags=["Root"])
async def root():
    """Корневой эндпоинт с информацией о API"""
    return {
        "message": "ToDo API with Hacker News Parser",
        "version": "1.0",
        "endpoints": {
            "tasks": "/tasks",
            "websocket": "/ws/tasks",
            "background_task": "/task-generator/run"
        }
    }


@app.get("/tasks", response_model=List[TaskResponse], tags=["Tasks"])
async def get_tasks(
        db: AsyncSession = Depends(get_db),
        done: Optional[bool] = None,
        category: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
):
    """Получить список задач с фильтрацией"""
    stmt = select(TaskModel)

    if done is not None:
        stmt = stmt.where(TaskModel.done == done)

    if category is not None:
        stmt = stmt.where(TaskModel.category == category)

    if source is not None:
        stmt = stmt.where(TaskModel.source == source)

    stmt = stmt.order_by(TaskModel.priority.desc(), TaskModel.created_at.desc())
    stmt = stmt.offset(offset).limit(limit)

    result = await db.execute(stmt)
    tasks = result.scalars().all()

    return tasks


@app.get("/tasks/{task_id}", response_model=TaskResponse, tags=["Tasks"])
async def get_task(task_id: int, db: AsyncSession = Depends(get_db)):
    """Получить задачу по ID"""
    stmt = select(TaskModel).where(TaskModel.id == task_id)
    result = await db.execute(stmt)
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    return task


@app.post("/tasks", response_model=TaskResponse, tags=["Tasks"])
async def create_task(
        task: TaskCreate,
        db: AsyncSession = Depends(get_db)
):
    """Создать новую задачу"""
    new_task = TaskModel(
        title=task.title,
        description=task.description,
        category=task.category,
        priority=task.priority if task.priority else 1,
        source="manual"
    )

    db.add(new_task)
    await db.commit()
    await db.refresh(new_task)

    # Отправляем уведомление через WebSocket
    await manager.broadcast({
        "type": "task_created",
        "message": f"Создана новая задача: {task.title[:50]}",
        "timestamp": datetime.utcnow().isoformat(),
        "task_id": new_task.id
    })

    logger.info(f"Создана задача: {task.title}")

    return new_task


@app.patch("/tasks/{task_id}", response_model=TaskResponse, tags=["Tasks"])
async def update_task(
        task_id: int,
        updated: TaskUpdate,
        db: AsyncSession = Depends(get_db)
):
    """Частичное обновление задачи"""
    # Получаем задачу
    stmt = select(TaskModel).where(TaskModel.id == task_id)
    result = await db.execute(stmt)
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    # Обновляем только переданные поля
    update_data = {}
    if updated.title is not None:
        update_data['title'] = updated.title
    if updated.description is not None:
        update_data['description'] = updated.description
    if updated.done is not None:
        update_data['done'] = updated.done
    if updated.category is not None:
        update_data['category'] = updated.category
    if updated.priority is not None:
        update_data['priority'] = updated.priority

    update_data['updated_at'] = datetime.utcnow()

    # Выполняем обновление
    stmt = (
        update(TaskModel)
        .where(TaskModel.id == task_id)
        .values(**update_data)
    )
    await db.execute(stmt)
    await db.commit()

    # Получаем обновленную задачу
    stmt = select(TaskModel).where(TaskModel.id == task_id)
    result = await db.execute(stmt)
    updated_task = result.scalar_one()

    # Отправляем уведомление
    await manager.broadcast({
        "type": "task_updated",
        "message": f"Обновлена задача #{task_id}: {updated_task.title[:50]}",
        "timestamp": datetime.utcnow().isoformat(),
        "task_id": task_id
    })

    logger.info(f"Обновлена задача #{task_id}")

    return updated_task


@app.delete("/tasks/{task_id}", tags=["Tasks"])
async def delete_task(task_id: int, db: AsyncSession = Depends(get_db)):
    """Удалить задачу"""
    # Проверяем существование задачи
    stmt = select(TaskModel).where(TaskModel.id == task_id)
    result = await db.execute(stmt)
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    # Удаляем задачу
    stmt = delete(TaskModel).where(TaskModel.id == task_id)
    await db.execute(stmt)
    await db.commit()

    # Отправляем уведомление
    await manager.broadcast({
        "type": "task_deleted",
        "message": f"Удалена задача #{task_id}: {task.title[:50]}",
        "timestamp": datetime.utcnow().isoformat(),
        "task_id": task_id
    })

    logger.info(f"Удалена задача #{task_id}")

    return {"message": "Задача успешно удалена"}


# ========== ФОНОВЫЕ ЗАДАЧИ ==========
@app.post("/task-generator/run", response_model=BackgroundTaskResponse, tags=["Background Tasks"])
async def run_background_task(background_tasks: BackgroundTasks):
    """Принудительный запуск фоновой задачи"""

    async def run_task_async():
        """Асинхронная функция для фоновой задачи"""
        try:
            logger.info("Ручной запуск фоновой задачи...")

            # Получаем данные из Hacker News
            stories = await fetch_top_stories()

            added_count = 0
            if stories:
                # Сохраняем в базу
                async with AsyncSessionLocal() as db:
                    added_count = await save_hn_stories_to_db(stories, db)
                    logger.info(
                        f"Ручная задача завершена. Обработано {len(stories)} новостей, добавлено {added_count} задач")

            return added_count

        except Exception as e:
            logger.error(f"Ошибка при ручном запуске задачи: {e}")
            return 0

    # Запускаем в фоне
    background_tasks.add_task(run_task_async)

    return {
        "message": "Фоновая задача запущена вручную",
        "task_count": 0,
        "status": "started"
    }


@app.get("/background-task/status", tags=["Background Tasks"])
async def get_background_task_status():
    """Получить статус фоновой задачи"""
    return {
        "running": background_task_running,
        "next_run_in_seconds": BACKGROUND_TASK_INTERVAL,
        "description": f"Фоновая задача автоматически запускается каждые {BACKGROUND_TASK_INTERVAL // 60} минут",
        "source": "Hacker News API"
    }


@app.get("/hacker-news/stats", tags=["Hacker News"])
async def get_hacker_news_stats(db: AsyncSession = Depends(get_db)):
    """Получить статистику по задачам из Hacker News"""
    stmt = select(TaskModel).where(TaskModel.source == "hacker_news")
    result = await db.execute(stmt)
    hn_tasks = result.scalars().all()

    total_tasks = len(hn_tasks)
    done_tasks = sum(1 for task in hn_tasks if task.done)

    # Статистика по категориям приоритета
    priority_stats = {}
    for task in hn_tasks:
        priority_stats[task.priority] = priority_stats.get(task.priority, 0) + 1

    return {
        "total_tasks": total_tasks,
        "done_tasks": done_tasks,
        "pending_tasks": total_tasks - done_tasks,
        "priority_stats": priority_stats,
        "last_updated": max([task.created_at for task in hn_tasks]) if hn_tasks else None
    }


# ========== ДОПОЛНИТЕЛЬНЫЕ ЭНДПОИНТЫ ==========
@app.get("/health", tags=["Health"])
async def health_check():
    """Проверка здоровья приложения"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "websocket_connections": len(manager.active_connections),
        "background_task_running": background_task_running
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True
    )