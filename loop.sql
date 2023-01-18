DO $$
DECLARE
	country_name text;
BEGIN
	country_name := 'country_test_name';

	FOR counter in 1..10
		LOOP
			INSERT INTO country(country_name)
			VALUES(country_name || counter);
		END LOOP;
END;
$$