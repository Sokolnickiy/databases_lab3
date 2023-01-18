import loguru
import re
import matplotlib.pyplot as plt
from databases import Database
from asyncio import run
import queries
import models
import asyncio

PORT=5432
HOST="localhost"
DB_NAME="postgress"
USER="postgres"
PASSWORD="postgres"


async def postgres_connection() -> Database:
    db = Database(
        url=f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}",
    )
    await db.connect()
    loguru.logger.debug("Returning postgres connection")
    return db


async def create_tables(conn: Database) -> None:
    await conn.execute(
        query=queries.DROP_COUNTRY
    )
    await conn.execute(
        query=queries.DROP_WEBSITE
    )
    await conn.execute(
        query=queries.DROP_CATEGORY
    )
    await conn.execute(
        query=queries.CREATE_TABLE_COUNTRY
    )
    await conn.execute(
        query=queries.CREATE_TABLE_CATEGORY
    )
    await conn.execute(
        query=queries.CREATE_TABLE_WEBSITE
    )
    loguru.logger.debug("Tables created")


async def populate_tables(conn: Database) -> None:
    await conn.execute(
        query=queries.POPULATE_COUNTRY
    )
    await conn.execute(
        query=queries.POPULATE_CATEGORY
    )
    await conn.execute(
        query=queries.POPULATE_WEBSITE
    )


async def create_views(conn: Database) -> None:
    await conn.execute(
        query=queries.COUNTRY_IMPACT_VIEW
    )
    await conn.execute(
        query=queries.CATEGORY_TYPE_POPULARITY_VIEW
    )
    await conn.execute(
        query=queries.COUNTRY_MOST_POPULAR_CATEGORY_VIEW
    )


async def get_category_popularity(conn: Database) -> list[models.CategoryPopularity]:
    data = await conn.fetch_all(
        query=queries.GET_CATEGORY_POPULARITY
    )
    return [models.CategoryPopularity.parse_obj(i) for i in data]


async def get_country_impact(conn: Database) -> list[models.CountryWebsiteAmount]:
    data = await conn.fetch_all(
        query=queries.GET_COUNTRY_IMPACT
    )
    return [models.CountryWebsiteAmount.parse_obj(i) for i in data]


async def get_country_most_popular_categories(conn: Database) -> list[models.CountryMostPopularCategory]:
    data = await conn.fetch_all(
        query=queries.GET_COUNTRY_MOST_POPULAR_CATEGORIES
    )
    return [models.CountryMostPopularCategory.parse_obj(i) for i in data]


async def print_data(conn: Database) -> None:
    category_popularity = await get_category_popularity(conn=conn)
    for i in category_popularity:
        print(f"\nPrinting entity:{i.__class__.__name__}")
        print(f"{i}\n")
    country_impact = await get_country_impact(conn=conn)
    for i in country_impact:
        print(f"\nPrinting entity:{i.__class__.__name__}")
        print(f"{i}\n")
    most_popular_categories = await get_country_most_popular_categories(conn=conn)
    for i in most_popular_categories:
        print(f"\nPrinting entity:{i.__class__.__name__}")
        print(f"{i}\n")


def clear_category_name(string: str) -> str:
    lst = (re.findall(r'>\s+(.+)', string) or string,)
    if type(lst[0]) == str:
        return lst[0]
    if type(lst[0]) == list:
        return lst[0][0]


async def build_diagram_plot(conn: Database) -> None:
    category_stats = await get_category_popularity(conn=conn)
    category_list = [clear_category_name(i.category_type) for i in category_stats]
    category_count_list = [i.count for i in category_stats]
    fig, ax = plt.subplots(figsize=(25, 20))
    plt.title("CATEGORY TYPE AMOUNT", fontsize=20)
    plt.xlabel("CATEGORIES", fontsize=20)
    plt.ylabel("CATEGORIES AMOUNT", fontsize=20)
    ax.bar(category_list, category_count_list, align="center", width=0.5)
    ax.tick_params(axis='x', labelrotation=90)
    plt.savefig("query1.png")


async def build_pie_chart(conn: Database) -> None:
    country_stats = await get_country_impact(conn=conn)
    names_list = [
        i.country_name
        for i in country_stats
    ]
    count_list = [i.count for i in country_stats]
    fig1, ax = plt.subplots()
    plt.title("TOP PRINCIPAL COUNTRIES")
    ax.pie(count_list, labels=names_list, autopct='%1.1f%%')
    plt.savefig("query2.png")


async def build_diagram_plot_for_6c_query(conn: Database) -> None:
    category_stats = await get_country_most_popular_categories(conn=conn)
    category_list = [clear_category_name(i.category_type) for i in category_stats]
    category_count_list = [i.category_count for i in category_stats]
    country_name_list = [i.country_name for i in category_stats]
    fig, ax = plt.subplots(figsize=(17, 17))
    plt.title("CATEGORY TYPE AMOUNT", fontsize=20)
    plt.xlabel("CATEGORIES", fontsize=20)
    plt.ylabel("CATEGORIES AMOUNT", fontsize=20)
    ax.bar(country_name_list, category_count_list)
    for i in range(len(category_stats)):
        plt.text(i, category_count_list[i], category_list[i], ha="center", fontsize=15, rotation=90)
    ax.tick_params(axis='x', labelrotation=45)
    plt.savefig("query3.png")


async def main() -> None:
    conn = await postgres_connection()
    await create_tables(conn=conn)
    await populate_tables(conn=conn)
    await create_views(conn=conn)
    await build_diagram_plot(conn=conn)
    await build_pie_chart(conn=conn)
    await build_diagram_plot_for_6c_query(conn=conn)


def run_main() -> None:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(main=main())


if __name__ == "__main__":
    run_main()
