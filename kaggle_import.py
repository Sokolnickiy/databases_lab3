import csv
from asyncio import run
from databases import Database
import asyncio

import queries
from main import (
    postgres_connection,
    build_pie_chart,
    build_diagram_plot_for_6c_query,
    build_diagram_plot,
    create_tables,
    create_views,
)


def read_file(file_path: str) -> tuple[list, list[dict]]:
    with open(file_path) as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        rows = []
        for row in csv_reader:
            values = {
                "site": row[1],
                "domain_name": row[2],
                "category": row[3],
                "country": row[4],
            }
            rows.append(values)
    return header, rows


def parse_country(rows: list) -> list:
    countries = []
    for i in rows:
        country = {
            "country_name": i["country"]
        }
        if country in countries:
            continue
        countries.append(country)
    return countries


def parse_category(rows: list) -> list:
    categories = []
    for i in rows:
        category = {
            "category_type": i["category"]
        }
        if category in categories:
            continue
        categories.append(category)
    return categories


async def populate_from_csv(rows: list, conn: Database) -> None:
    categories = parse_category(rows)
    countries = parse_country(rows)
    for i in categories:
        await conn.execute(
            query=queries.INSERT_CATEGORIES_FROM_CSV,
            values=i
        )
    for i in countries:
        await conn.execute(
            query=queries.INSERT_COUNTRIES_FROM_CSV,
            values=i
        )
    for i in rows:
        category = {"category": i.pop("category")}
        category_id = await conn.execute(
            query=queries.GET_CATEGORY_ID,
            values=category
        )
        country = {"country": i.pop("country")}
        country_id = await conn.execute(
            query=queries.GET_COUNTRY_ID,
            values=country
        )
        i["principal_country"] = country_id
        i["category_id"] = category_id
        await conn.execute(
            query=queries.INSERT_INTO_WEBSITES_FROM_CSV,
            values=i
        )


async def kaggle_import() -> None:
    conn = await postgres_connection()
    file = read_file(file_path="C:/Users/sokol/Desktop/БД/лаба3/lab03websites/df_1.csv")
    await create_tables(conn=conn)
    await populate_from_csv(rows=file[1], conn=conn)
    await create_views(conn=conn)
    await build_diagram_plot(conn=conn)
    await build_pie_chart(conn=conn)
    await build_diagram_plot_for_6c_query(conn=conn)


def run_import() -> None:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(main=kaggle_import())


if __name__ == "__main__":
    run_import()
