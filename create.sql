CREATE TABLE country(
    "id" int GENERATED ALWAYS AS IDENTITY,
    "country_name" VARCHAR(50),
    PRIMARY KEY ("id")
);

CREATE TABLE category(
    "id" int GENERATED ALWAYS AS IDENTITY,
    "category_type" text,
    PRIMARY KEY ("id")
);

CREATE TABLE website (
    "id" int GENERATED ALWAYS AS IDENTITY,
    "principal_country" int REFERENCES country("id"),
    "site" VARCHAR(50),
    "category_id" int REFERENCES category("id"),
    "domain_name" VARCHAR(50),
    PRIMARY KEY ("id")
);