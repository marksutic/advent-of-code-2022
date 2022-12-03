## Usage
start database with 

`docker compose up -d`

connect to db with psql:

`psql postgresql://aoc2022:aoc2022@localhost:55432/aoc2022`

run each day solution with:

`\i sql/day01/solution.sql`