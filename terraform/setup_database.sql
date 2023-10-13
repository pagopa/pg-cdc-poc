CREATE TABLE status (
    id SERIAL PRIMARY KEY,
    value VARCHAR(255)
);

INSERT INTO status (value) VALUES ('active'), ('inactive');

CREATE TABLE students (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    date_of_birth DATE,
    status_id INT,
    FOREIGN KEY (status_id) REFERENCES status(id)
);

CREATE PUBLICATION students FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('slot_students', 'pgoutput');