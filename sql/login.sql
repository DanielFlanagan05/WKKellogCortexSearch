USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS;
USE SCHEMA DATA;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    username STRING UNIQUE NOT NULL,
    password_hash STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS user_prompts (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    prompt_text STRING NOT NULL
);
