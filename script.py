import asyncio
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
    """Подключение к БД"""
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
    """
    Возвращает одну запись из БД\n
    query - Запрос на SQL\n
    params - Параметры для запроса\n
    Возвращает объект aiosqlite.Row, либо None, если запись не найдена
    """
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await foreign_keys_on(db)
        async with db.execute(query, params) as cur:
            return await cur.fetchone()


async def db_fetchall(query: str, params: tuple = ()) -> List[aiosqlite.Row]:
    """
    Возвращает несколько записей из БД\n
    query - Запрос на SQL\n
    params - Параметры для запроса\n
    Возвращает объект List[aiosqlite.Row]
    """
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await foreign_keys_on(db)
        async with db.execute(query, params) as cur:
            return await cur.fetchall()


async def db_execute(query: str, params: tuple = ()):
    """
    Выполняет один SQL запрос в БД с одним набором параметров\n
    query - Запрос на SQL\n
    params - Параметры для запроса\n
    """
    async with aiosqlite.connect(db_path) as db:
        await foreign_keys_on(db)
        await db.execute(query, params)
        await db.commit()


async def db_executemany(query: str, seq_params: list[tuple]):
    """
    Выполняет один и тот же SQL запрос в БД с разными параметрами\n
    query - Запрос на SQL\n
    params - Параметры для запроса\n
    """
    async with aiosqlite.connect(db_path) as db:
        await foreign_keys_on(db)
        await db.executemany(query, seq_params)
        await db.commit()
