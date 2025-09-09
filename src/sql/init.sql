-- source data --
CREATE TABLE IF NOT EXISTS source_data (
    id SERIAL PRIMARY KEY,
    energy_100g DOUBLE PRECISION,
    fat_100g DOUBLE PRECISION,
    carbohydrates_100g DOUBLE PRECISION,
    proteins_100g DOUBLE PRECISION,
    sugars_100g DOUBLE PRECISION
);

-- samples --
INSERT INTO source_data (energy_100g, fat_100g, carbohydrates_100g, proteins_100g, sugars_100g) VALUES
(250.0, 10.0, 30.0, 5.0, 8.0),
(300.0, 12.0, 40.0, 8.0, 12.0),
(180.0, 5.0, 20.0, 3.0, 4.0),
(-1.0, 0.0, 0.0, 0.0, 0.0); -- -1 used to simulate invalid value -> will become NULL

-- predictions table --
CREATE TABLE IF NOT EXISTS predictions (
    id BIGINT,
    prediction INTEGER,
    run_id VARCHAR(64),
    run_ts VARCHAR(64)
);
