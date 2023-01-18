import re
from asyncio import run
import asyncio

import loguru
from databases import Database
import matplotlib.pyplot as plt
from main import (
    postgres_connection,
    get_category_popularity,
    get_country_impact,
    get_country_most_popular_categories
)


def clear_category_name(string: str) -> str:
    lst = (re.findall(r'>\s+(.+)', string) or string,)
    loguru.logger.debug(lst)
    if type(lst[0]) == str:
        return lst[0]
    if type(lst[0]) == list:
        return lst[0][0]


async def build_diagram_plot(conn: Database) -> None:
    category_stats = await get_category_popularity(conn=conn)
    category_list = [clear_category_name(i.category_type) for i in category_stats]
    category_count_list = [i.count for i in category_stats]
    fig, ax = plt.subplots(figsize=(17, 17))
    plt.title("CATEGORY TYPE AMOUNT", fontsize=20)
    plt.xlabel("CATEGORIES", fontsize=20)
    plt.ylabel("CATEGORIES AMOUNT", fontsize=20)
    ax.bar(category_list, category_count_list)
    ax.tick_params(axis='x', labelrotation=45)
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
        plt.text(i, category_count_list[i], category_list[i], ha="center", fontsize=15)
    ax.tick_params(axis='x', labelrotation=45)
    plt.savefig("query3.png")


async def visualize() -> None:
    conn = await postgres_connection()
    await build_diagram_plot(conn=conn)
    await build_pie_chart(conn=conn)
    await build_diagram_plot_for_6c_query(conn=conn)


def run_visualize() -> None:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(main=visualize())


if __name__ == "__main__":
    run_visualize()
