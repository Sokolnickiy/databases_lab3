DROP_COUNTRY = """
    DROP TABLE IF EXISTS country CASCADE;
"""

DROP_CATEGORY = """
    DROP TABLE IF EXISTS category CASCADE;
"""

DROP_WEBSITE = """
    DROP TABLE IF EXISTS website CASCADE;
"""


CREATE_TABLE_COUNTRY = """
    CREATE TABLE country(
        "id" int GENERATED ALWAYS AS IDENTITY,
        "country_name" VARCHAR(50),
        PRIMARY KEY ("id")
    );
"""

CREATE_TABLE_CATEGORY = """
    CREATE TABLE category(
        "id" int GENERATED ALWAYS AS IDENTITY,
        "category_type" text,
        PRIMARY KEY ("id")
    );
"""

CREATE_TABLE_WEBSITE = """
    CREATE TABLE website (
        "id" int GENERATED ALWAYS AS IDENTITY,
        "principal_country" int REFERENCES country("id"),
        "site" VARCHAR(50),
        "category_id" int REFERENCES category("id"),
        "domain_name" VARCHAR(50),
        PRIMARY KEY ("id")
    );
"""


POPULATE_COUNTRY = """
    INSERT INTO country("country_name") VALUES
        ('United States'),
        ('China'),
        ('Russia'),
        ('Czech Republic');
    
"""

POPULATE_CATEGORY = """
    INSERT INTO category("category_type") VALUES
        ('Computers Electronics and Technology > Search Engines'),
        ('Arts & Entertainment > Streaming & Online TV'),
        ('Computers Electronics and Technology > Social Media Networks'),
        ('Reference Materials > Dictionaries and Encyclopedias'),
        ('News & Media Publishers'),
        ('Adult');
"""

POPULATE_WEBSITE = """
    INSERT INTO website(
        "principal_country",
        "site",
        "category_id",
        "domain_name"
    ) VALUES
        (1, 'Google Search', 1, 'google.com'),
        (1, 'YouTube', 2, 'youtube.com'),
        (1, 'Facebook', 3, 'facebook.com'),
        (1, 'Twitter', 3, 'twitter.com'),
        (1, 'Instagram', 3, 'instagram.com'),
        (2, 'Baidu', 1, 'baidu.com'),
        (1, 'Wikipedia', 4, 'wikipedia.org'),
        (3, 'Yandex', 1, 'yandex.ru'),
        (1, 'Yahoo', 5, 'yahoo.com'),
        (4, 'xVideos', 6, 'xvideos.com'),
        (1, 'WhatsApp', 3, 'whatsapp.com');
"""

CATEGORY_TYPE_POPULARITY_VIEW = """
    CREATE VIEW category_amount AS
        SELECT count(*), c.category_type FROM website
        JOIN category c ON c.id = website.category_id
        GROUP BY c.category_type;
"""

COUNTRY_IMPACT_VIEW = """
    CREATE VIEW country_website_amount AS
        SELECT count(*), c.country_name FROM website
        JOIN country c on c.id = website.principal_country
        GROUP BY c.country_name;
"""

COUNTRY_MOST_POPULAR_CATEGORY_VIEW = """
    CREATE VIEW country_popular_category AS
        SELECT DISTINCT ON(country_name) * from (
            SELECT  count(*) as category_count, cou.country_name, cat.category_type FROM website
            JOIN category cat ON cat.id = website.category_id
            JOIN country cou ON cou.id = website.principal_country
            GROUP BY cou.country_name, cat.category_type
            ) q
        ORDER BY country_name, category_count DESC;
"""

GET_CATEGORY_POPULARITY = """
    SELECT * FROM category_amount;
"""

GET_COUNTRY_IMPACT = """
    SELECT * from country_website_amount;
"""

GET_COUNTRY_MOST_POPULAR_CATEGORIES = """
    SELECT * from country_popular_category;
"""

INSERT_COUNTRIES_FROM_CSV = """
    INSERT INTO country(
        country_name
    ) VALUES (
        :country_name
    );
"""

INSERT_CATEGORIES_FROM_CSV = """
    INSERT INTO category(
        category_type
    ) VALUES (
        :category_type
    );
"""

INSERT_INTO_WEBSITES_FROM_CSV = """
    INSERT INTO website(
        principal_country,
        site,
        category_id,
        domain_name
    ) VALUES (
        :principal_country,
        :site,
        :category_id,
        :domain_name
    );
"""

GET_CATEGORY_ID = """
    SELECT id from category where category_type=:category;
"""

GET_COUNTRY_ID = """
    SELECT id from country where country_name=:country;
"""

GET_COUNTRIES = """
    SELECT * from country;
"""

GET_CATEGORIES = """
    SELECT * from category;
"""

GET_WEBSITES = """
    SELECT * from website;
"""