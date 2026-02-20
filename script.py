import asyncio
import aiosqlite
from contextlib import asynccontextmanager
import datetime as dt
from typing import Optional, List, Dict, Any

import httpx
import uvicorn as uvi
from fastapi import FastAPI, HTTPException, Query, params
from pydantic import BaseModel, Field, field_validator


# ! Класс БД, чтобы все методы были рядом
class DataBase:
    """Класс для работы с базой данных SQLite. Содержит методы для инициализации БД и выполнения запросов."""
    
    def __init__(self):
        self.__queries = {
            # ? name - название города, lat - широта, lon - долгота
            "cities":   """
                            CREATE TABLE IF NOT EXISTS cities(
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name TEXT NOT NULL UNIQUE,
                                lat REAL NOT NULL,
                                lon REAL NOT NULL
                            );
                        """,
                        
            # ? city_id - ID города, date - Дата, time - время, temp - температура, 
            # ? wind_speed - скорость ветра, precipitation - кол-во осадков (мм),
            # ? update_at - время обновления
            "forecast": """
                        CREATE TABLE IF NOT EXISTS forecast_hourly(
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                city_id INTEGER NOT NULL,
                                date TEXT NOT NULL,
                                time TEXT NOT NULL,
                                temp REAL,
                                wind_speed REAL,
                                precipitation REAL,
                                updated_at TEXT NOT NULL,
                                UNIQUE(city_id, time),
                                FOREIGN KEY(city_id) REFERENCES cities(id) ON DELETE CASCADE
                            );
                        """,
                        
            # ? Индексация (для ускорения поиска)
            "index":    """
                        CREATE INDEX IF NOT EXISTS idx_forecast ON forecast_hourly(city_id, date, time)
                        """
        }
        
    
    async def foreign_keys_on(self, db: aiosqlite.Connection):
        """Включение поддержки внешних ключей"""
        await db.execute("PRAGMA foreign_keys = ON;")

    # * подключение к БД
    async def init_db(self):
        """Подключение к БД и создание таблиц, если их нет"""
        async with aiosqlite.connect(db_path) as database:
            await self.foreign_keys_on(database)
            
            # создаем таблицу для хранения городов
            await database.execute(self.__queries["cities"])
            
            # создаем таблицу для каждодневных прогнозов погоды
            await database.execute(self.__queries["forecast"])
            
            # создаем индексы для ускорения поиска
            await database.execute(self.__queries["index"])

            await database.commit()

    async def db_fetchone(self, query: str, params: tuple = ()) -> Optional[aiosqlite.Row]:
        """Возвращает одну запись из БД по SQL запросу и параметрам
        Args:
            query (str): Запрос на SQL
            params (tuple): Параметры для запроса
        """
        async with aiosqlite.connect(db_path) as database:
            database.row_factory = aiosqlite.Row
            await self.foreign_keys_on(database)
            async with database.execute(query, params) as cur:
                return await cur.fetchone()

    async def db_fetchall(self, query: str, params: tuple = ()) -> List[aiosqlite.Row]:
        """Возвращает несколько записей из БД по SQL запросу и параметрам
        Args:
            query (str): Запрос на SQL
            params (tuple): Параметры для запроса
        """
        async with aiosqlite.connect(db_path) as database:
            database.row_factory = aiosqlite.Row
            await self.foreign_keys_on(database)
            async with database.execute(query, params) as cur:
                return await cur.fetchall()

    async def db_execute(self, query: str, params: tuple = ()):
        """Выполняет один SQL запрос в БД с одним набором параметров
        Args:
            query (str): Запрос на SQL
            params (tuple): Параметры для запроса
        """
        async with aiosqlite.connect(db_path) as database:
            await self.foreign_keys_on(database)
            await database.execute(query, params)
            await database.commit()

    async def db_executemany(self, query: str, seq_params: list[tuple]):
        """Выполняет один и тот же SQL запрос в БД с разными параметрами (несколько раз)
        Args:
            query (str): Запрос на SQL
            seq_params (list[tuple]): Параметры для запроса
        """
        async with aiosqlite.connect(db_path) as database:
            await self.foreign_keys_on(database)
            await database.executemany(query, seq_params)
            await database.commit()



# ! Класс запросов к Open-Meteo, чтобы все методы были рядом
class OpenMeteoAPI:
    """Класс для работы с API Open-Meteo"""
    
    async def fetch_hourly_today(self, lat: float, lon: float):
        """Получает почасовой прогноз на текущий день по широте и долготе
        
        Args:
            lat (float): Широта
            lon (float): Долгота
        """
        today = dt.date.today().isoformat()
        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            'start_date': today,
            'end_date': today,
            'timezone': "auto"
        }
        
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
        
        hourly = data.get("hourly", {})
        
        result = {
            "date": today,
            "times": hourly.get("time", []),
            "temps": hourly.get("temperature_2m", []),
            "humidities": hourly.get("relative_humidity_2m", []),
            "wind_speeds": hourly.get("wind_speed_10m", []),
            "precipitations": hourly.get("precipitation", []),
        }
        return result
    
    async def fetch_current_weather(self, lat: float, lon: float):
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
            response = await client.get(api_url, params=params)
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



# * ПЕРЕМЕННЫЕ
db_path = "weather.db"
api_url = "https://api.open-meteo.com/v1/forecast"


db = DataBase()
"""Объект класса DataBase для работы с базой данных"""

api_base = OpenMeteoAPI()
"""Объект класса OpenMeteoAPI для работы с API Open-Meteo"""

host_name = "127.0.0.1"
host_port = 8000




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







# * FAST API

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Контекстный менеджер для запуска и остановки приложения FastAPI. Инициализирует БД при запуске."""
    await db.init_db()
    
    app.state.stop_event = asyncio.Event()
    yield

    app.state.stop_event.set()


app = FastAPI(
    title='Погодный API',
    description='API получения погоды',
    version='1.0.0',
    lifespan=lifespan
)


# * ENDPOINTS

@app.get("/weather/current", response_model=CurrentWeatherOut)
async def get_current_weather(
    lat: float = Query(..., description="Широта"), lon: float = Query(..., description="Долгота")):
    """Получает текущую погоду по широте и долготе"""
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=400, detail="Недопустимое значение широты. Должно быть от -90 до 90.")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=400, detail="Недопустимое значение долготы. Должно быть от -180 до 180.")
    
    try:
        data = await api_base.fetch_current_weather(lat, lon)
        return CurrentWeatherOut(**data)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Ошибка при получении данных от внешнего API: {str(e)}")


@app.post("/cities", response_model=CityOut, status_code=201)
async def add_city(payload: AddCityIn):
    """Сохраняет город в БД"""
    query = "INSERT INTO cities(name, lat, lon) VALUES (?, ?, ?);"
    params = (payload.name, payload.lat, payload.lon)
    try:
        await db.db_execute(query, params)
    except aiosqlite.IntegrityError:
        raise HTTPException(status_code=400, detail="Город с таким названием уже существует")
    
    query = "SELECT name, lat, lon FROM cities WHERE name = ?;"
    city = await db.db_fetchone(query, (payload.name,))
    return CityOut(name=city["name"], lat=city["lat"], lon=city["lon"])


@app.get("/cities", response_model=List[CityOut])
async def get_cities():
    """Получает список всех городов из БД"""
    query = "SELECT name, lat, lon FROM cities ORDER BY name ASC;"
    cities = await db.db_fetchall(query)
    return [CityOut(name=city["name"], lat=city["lat"], lon=city["lon"]) for city in cities]


# ! запуск приложения
if __name__ == "__main__":
    uvi.run(app, host=host_name, port=host_port, reload=False)
