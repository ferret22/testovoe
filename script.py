from ast import Add
import asyncio
from calendar import c
from time import timezone
import aiosqlite
from contextlib import asynccontextmanager
import datetime as dt
from typing import Optional, List, Dict, Any

import httpx
import uvicorn as uvi
from fastapi import FastAPI, HTTPException, Query, params
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
                        CREATE TABLE IF NOT EXISTS cities(
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL UNIQUE,
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
    source_time: Optional[str]
    temp_unit: Optional[str]
    wind_unit: Optional[str]
    pressure_unit: Optional[str]
    timezone: Optional[str]


class AddCityIn(BaseModel):
    """Модель для добавления города в БД"""
    name: str = Field(..., min_length=1, max_length=100, description="Название города")
    lat: float = Field(..., ge=-90, le=90, description="Широта города")
    lon: float = Field(..., ge=-180, le=180, description="Долгота города")
    
    @field_validator("name")
    @classmethod
    def validate_name(cls, value):
        """Проверяет, что название города не состоит только из пробелов"""
        if not value.strip():
            raise ValueError("Название города не может быть пустым")
        return value.strip()


class CityOut(BaseModel):
    """Модель для вывода информации о городе"""
    name: str
    lat: float
    lon: float


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
    current_units = data.get("current_units", {})

    result = {
        "lat": lat,
        "lon": lon,
        "temp": current_weather.get("temperature_2m"),
        "wind_speed": current_weather.get("wind_speed_10m"),
        "pressure": current_weather.get("pressure_msl"),
        "source_time": current_weather.get("time"),
        "temp_unit": current_units.get("temperature_2m"),
        "wind_unit": current_units.get("wind_speed_10m"),
        "pressure_unit": current_units.get("pressure_msl"),
        "timezone": data.get("timezone_abbreviation")
    }
    return result


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
    """Получает текущую погоду по широте и долготе"""
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=400, detail="Недопустимое значение широты. Должно быть от -90 до 90.")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=400, detail="Недопустимое значение долготы. Должно быть от -180 до 180.")
    
    try:
        data = await fetch_current_weather(lat, lon)
        return CurrentWeatherOut(**data)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Ошибка при получении данных от внешнего API: {str(e)}")


@app.post("/cities", response_model=CityOut, status_code=201)
async def add_city(payload: AddCityIn):
    """Сохраняет город в БД"""
    query = "INSERT INTO cities(name, lat, lon) VALUES (?, ?, ?);"
    params = (payload.name, payload.lat, payload.lon)
    try:
        await db_execute(query, params)
    except aiosqlite.IntegrityError:
        raise HTTPException(status_code=400, detail="Город с таким названием уже существует")
    
    query = "SELECT name, lat, lon FROM cities WHERE name = ?;"
    city = await db_fetchone(query, (payload.name,))
    return CityOut(name=city["name"], lat=city["lat"], lon=city["lon"])


@app.get("/cities", response_model=List[CityOut])
async def get_cities():
    """Получает список всех городов из БД"""
    query = "SELECT name, lat, lon FROM cities ORDER BY name ASC;"
    cities = await db_fetchall(query)
    return [CityOut(name=city["name"], lat=city["lat"], lon=city["lon"]) for city in cities]


# ! запуск приложения
if __name__ == "__main__":
    uvi.run(app, host="127.0.0.1", port=8000, reload=False)
