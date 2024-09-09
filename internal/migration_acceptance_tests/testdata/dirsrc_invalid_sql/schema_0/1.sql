CREATE TABLE fizzbuzz (
    id TEXT REFERENCES non_existent_table (id)
);
