import asyncio
import aiohttp
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from table_one import SpimexTradingResult
from dotenv import load_dotenv
import os
import time
import tempfile

load_dotenv()

DATABASE_URL = os.getenv("ASYNC_DATABASE_URL")
BASE_URL = "https://spimex.com"
RESULTS_URL = "https://spimex.com/markets/oil_products/trades/results/"
START_YEAR = 2023
LIMIT_FILES = 20

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)


async def fetch_html(session, url):
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()


async def get_bulletin_urls(session):
    page_number = 1
    data = []

    while len(data) < LIMIT_FILES:
        page_url = f"{RESULTS_URL}?page=page-{page_number}" if page_number > 1 else RESULTS_URL
        html = await fetch_html(session, page_url)
        soup = BeautifulSoup(html, 'html.parser')
        found_new = False

        for link in soup.find_all("a", href=True):
            if "Бюллетень по итогам торгов в Секции «Нефтепродукты»" in link.text:
                date_span = link.find_next("span")
                if date_span and date_span.text.strip():
                    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", date_span.text.strip())
                    if match:
                        day, month, year = match.groups()
                        trade_date = datetime(int(year), int(month), int(day))
                        if trade_date.year < START_YEAR:
                            return data
                        data.append((BASE_URL + link['href'], trade_date))
                        found_new = True
                        if len(data) >= LIMIT_FILES:
                            break

        if not found_new:
            break
        page_number += 1

    return data


async def download_and_parse(session, url):
    async with session.get(url) as resp:
        content = await resp.read()

    try:
        with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        df_dict = pd.read_excel(tmp_path, sheet_name=None, engine="xlrd", header=None)
    except:
        return None

    for data in df_dict.values():
        for i, row in data.iterrows():
            if any(isinstance(cell, str) and "Единица измерения: Метрическая тонна" in cell for cell in row):
                df_clean = data.iloc[i + 1:].reset_index(drop=True)
                df_clean.columns = df_clean.iloc[0]
                df_clean = df_clean[1:].reset_index(drop=True)
                df_clean.columns = df_clean.columns.astype(str).str.replace('\n', ' ').str.strip()
                return df_clean

    return None


def process_dataframe(df, trade_date):
    expected_columns = ['Код Инструмента', 'Наименование Инструмента', 'Базис поставки',
                        'Объем Договоров в единицах измерения', 'Обьем Договоров, руб.', 'Количество Договоров, шт.']

    if any(col not in df.columns for col in expected_columns):
        return None

    df = df.copy()

    df['Количество Договоров, шт.'] = df['Количество Договоров, шт.'].astype(str).str.replace(',', '').str.strip()
    df['Объем Договоров в единицах измерения'] = df['Объем Договоров в единицах измерения'].astype(str).str.replace(',', '').str.strip()
    df['Обьем Договоров, руб.'] = df['Обьем Договоров, руб.'].astype(str).str.replace(',', '').str.strip()

    df['count'] = pd.to_numeric(df['Количество Договоров, шт.'], errors='coerce').fillna(0).astype(int)
    df['volume'] = pd.to_numeric(df['Объем Договоров в единицах измерения'], errors='coerce')
    df['total'] = pd.to_numeric(df['Обьем Договоров, руб.'], errors='coerce')

    df = df[df['count'] > 0]
    df = df[df['volume'].notna()]
    df = df[df['total'].notna()]
    df = df[~df['Код Инструмента'].astype(str).str.contains('Итого', na=False)]

    df = df.rename(columns={
        'Код Инструмента': 'exchange_product_id',
        'Наименование Инструмента': 'exchange_product_name',
        'Базис поставки': 'delivery_basis_name'
    })

    df['oil_id'] = df['exchange_product_id'].astype(str).str[:4]
    df['delivery_basis_id'] = df['exchange_product_id'].astype(str).str[4:7]
    df['delivery_type_id'] = df['exchange_product_id'].astype(str).str[-1]
    df['date'] = trade_date.replace(tzinfo=None)

    now = datetime.now()
    df['created_on'] = now
    df['updated_on'] = now

    return df[['exchange_product_id', 'exchange_product_name', 'oil_id', 'delivery_basis_id',
               'delivery_basis_name', 'delivery_type_id', 'volume', 'total', 'count', 'date',
               'created_on', 'updated_on']]


async def save_to_db(df):
    async with AsyncSessionLocal() as session:
        for _, row in df.iterrows():
            record = SpimexTradingResult(**row.to_dict())
            try:
                session.add(record)
            except IntegrityError:
                await session.rollback()
        await session.commit()


async def main():
    start = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        urls = await get_bulletin_urls(session)
        tasks = [download_and_parse(session, url) for url, _ in urls]
        results = await asyncio.gather(*tasks)

        for (_, date), df in zip(urls, results):
            if df is not None:
                processed = process_dataframe(df, date)
                if processed is not None:
                    await save_to_db(processed)

    end = time.perf_counter()
    print(f"Выполнено за {end - start:.2f} секунд")


if __name__ == "__main__":
    asyncio.run(main())
