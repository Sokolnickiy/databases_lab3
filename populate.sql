INSERT INTO category("category_type") VALUES
    ('Computers Electronics and Technology > Search Engines'),
    ('Arts & Entertainment > Streaming & Online TV'),
    ('Computers Electronics and Technology > Social Media Networks'),
    ('Reference Materials > Dictionaries and Encyclopedias'),
    ('News & Media Publishers'),
    ('Adult');


INSERT INTO country("country_name") VALUES
    ('United States'),
    ('China'),
    ('Russia'),
    ('Czech Republic');


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