--category_type popularity for diagram plot
SELECT count(*), c.category_type FROM website
JOIN category c ON c.id = website.category_id
GROUP BY c.category_type;


--countries website amount for pie chart
SELECT count(*), c.country_name FROM website
JOIN country c on c.id = website.principal_country
GROUP BY c.country_name;


--country's most popular category
SELECT DISTINCT ON(country_name) * from (
	SELECT  count(*) as category_count, cou.country_name, cat.category_type FROM website
	JOIN category cat ON cat.id = website.category_id
	JOIN country cou ON cou.id = website.principal_country
	GROUP BY cou.country_name, cat.category_type
	) q
ORDER BY country_name, category_count DESC;