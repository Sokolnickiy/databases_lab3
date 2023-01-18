import json
from databases import Database
from asyncio import run
import queries
import models
import asyncio
from main import (
    postgres_connection,
    create_tables,
)
from kaggle_import import read_file, populate_from_csv


async def get_data(conn: Database) -> dict:
    categories = await conn.fetch_all(query=queries.GET_CATEGORIES)
    categories = [models.Category.parse_obj(i) for i in categories]
    countries = await conn.fetch_all(query=queries.GET_COUNTRIES)
    countries = [models.Country.parse_obj(i) for i in countries]
    websites = await conn.fetch_all(query=queries.GET_WEBSITES)
    websites = [models.Website.parse_obj(i) for i in websites]
    return {
        "categories": categories,
        "countries": countries,
        "websites": websites
    }


async def export_json() -> None:
    conn = await postgres_connection()
    file = read_file(file_path="C:/Users/sokol/Desktop/БД/лаба3/lab03websites/result.json")
    await create_tables(conn=conn)
    await populate_from_csv(conn=conn, rows=file[1])
    data = await get_data(conn=conn)
    with open("result.json", "w") as file:
        json.dump(data, file, default=str)


def run_export_json() -> None:
    run(main=export_json())


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run_export_json()
