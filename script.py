import asyncio
from calendar import c
import aiosqlite
from contextlib import asynccontextmanager
import datetime as dt
from typing import Optional, List, Dict, Any

import httpx
import uvicorn as uvi
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator


# * ПЕРЕМЕННЫЕ
db_path = "weather.db"
base_url = "https://api.open-meteo.com/v1/forecast"


# * подключение к БД

async def foreign_keys_on(db: aiosqlite.Connection):
    """Включение поддержки внешних ключей"""
    await db.execute("PRAGMA foreign_keys = ON;")


async def init_db():
    """Подключение к БД и создание таблиц, если их нет"""
    async with aiosqlite.connect(db_path) as db:
        # 
        await foreign_keys_on(db)
        
        # создаем таблицу для хранения городов
        # * name - название города, lat - широта, lon - долгота
        await db.execute("""
                        CREATE TABLE IF NOT EXIST cities(
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            lat REAL NOT NULL,
                            lon REAL NOT NULL
                        );
                        """)
        
        
        await db.commit()


async def db_fetchone(query: str, params: tuple = ()) -> Optional[aiosqlite.Row]:
    """Возвращает одну запись из БД по SQL запросу и параметрам
    Args:
        query (str): Запрос на SQL
        params (tuple): Параметры для запроса
    """
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await foreign_keys_on(db)
        async with db.execute(query, params) as cur:
            return await cur.fetchone()


async def db_fetchall(query: str, params: tuple = ()) -> List[aiosqlite.Row]:
    """Возвращает несколько записей из БД по SQL запросу и параметрам
    Args:
        query (str): Запрос на SQL
        params (tuple): Параметры для запроса
    """
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await foreign_keys_on(db)
        async with db.execute(query, params) as cur:
            return await cur.fetchall()


async def db_execute(query: str, params: tuple = ()):
    """Выполняет один SQL запрос в БД с одним набором параметров
    Args:
        query (str): Запрос на SQL
        params (tuple): Параметры для запроса
    """
    async with aiosqlite.connect(db_path) as db:
        await foreign_keys_on(db)
        await db.execute(query, params)
        await db.commit()


async def db_executemany(query: str, seq_params: list[tuple]):
    """Выполняет один и тот же SQL запрос в БД с разными параметрами (несколько раз)
    Args:
        query (str): Запрос на SQL
        seq_params (list[tuple]): Параметры для запроса
    """
    async with aiosqlite.connect(db_path) as db:
        await foreign_keys_on(db)
        await db.executemany(query, seq_params)
        await db.commit()


# * pydantic models
class CurrentWeatherOut(BaseModel):
    """Модель для вывода текущей погоды"""
    lat: float
    lon: float
    temp: Optional[float]
    wind_speed: Optional[float]
    pressure: Optional[float]
    source_time: Optional[float]


# * Запросы к OPEN-METEO
async def fetch_current_weather(lat: float, lon: float):
    """Получает текущую погоду по широте и долготе

    Args:
        lat (float): Широта
        lon (float): Долгота
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,wind_speed_10m,pressure_msl",
        "timezone": "auto"
    }
    
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
    
    current_weather = data.get("current", {})
    print(data)
    print(current_weather)


# * FAST API

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Контекстный менеджер для запуска и остановки приложения FastAPI. Инициализирует БД при запуске."""
    await init_db()
    
    app.state.stop_event = asyncio.Event()
    yield

    app.state.stop_event.set()


app = FastAPI(
    title='Погодный API',
    description='API получения погоды',
    version='1.0.0',
    lifespan=lifespan
)


# * endpoints

@app.get("/weather/current", response_model=CurrentWeatherOut)
async def get_current_weather(
    lat: float = Query(..., description="Широта"),
    lon: float = Query(..., description="Долгота")
):
    """Получает текущую погоду по широте и долготе
    Args:
        lat (float): Широта
        lon (float): Долгота
    """
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=400, detail="Недопустимое значение широты. Должно быть от -90 до 90.")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=400, detail="Недопустимое значение долготы. Должно быть от -180 до 180.")
    
    try:
        await fetch_current_weather(lat, lon)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Ошибка при получении данных от внешнего API: {str(e)}")

