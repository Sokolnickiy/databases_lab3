from pydantic import BaseModel


class CategoryPopularity(BaseModel):
    count: int
    category_type: str


class CountryWebsiteAmount(BaseModel):
    count: int
    country_name: str


class CountryMostPopularCategory(BaseModel):
    category_count: int
    country_name: str
    category_type: str


class Country(BaseModel):
    id: int
    country_name: str


class Category(BaseModel):
    id: int
    category_type: str


class Website(BaseModel):
    id: int
    principal_country: int
    category_id: int
    site: str
    domain_name: str
