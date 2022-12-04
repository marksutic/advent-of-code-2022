drop schema if exists DAY_02 cascade;
create schema DAY_02;

create table day_02.INPUT (
  PLAY     text check(PLAY in ('A', 'B', 'C')),
  RESPONSE text check(RESPONSE in ('X', 'Y', 'Z'))
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY day_02.INPUT from 'sql/day02/input.txt' with (FORMAT 'csv', delimiter ' ');

/*
 * A-rock     X-rock
 * B-paper    Y-paper
 * C-scissors Z-scissors
*/
\echo 'First star:'
with score_results (play, response, score) as (
    values ('A', 'X', 1 + 3),
           ('A', 'Y', 2 + 6),
           ('A', 'Z', 3 + 0),
           ('B', 'X', 1 + 0),
           ('B', 'Y', 2 + 3),
           ('B', 'Z', 3 + 6),
           ('C', 'X', 1 + 6),
           ('C', 'Y', 2 + 0),
           ('C', 'Z', 3 + 3)
)
select sum(sr.SCORE) as RESULT
from day_02.INPUT p
join SCORE_RESULTS sr
  on p.PLAY = sr.PLAY
  and p.RESPONSE = sr.RESPONSE;

/*
 * A-rock     X-lose
 * B-paper    Y-draw
 * C-scissors Z-win
 * rock 1
 * paper 2
 * scissors 3
*/
\echo 'Second star:'
with score_results (play, response, score) as (
    values ('A', 'X', 3 + 0),
           ('A', 'Y', 1 + 3),
           ('A', 'Z', 2 + 6),
           ('B', 'X', 1 + 0),
           ('B', 'Y', 2 + 3),
           ('B', 'Z', 3 + 6),
           ('C', 'X', 2 + 0),
           ('C', 'Y', 3 + 3),
           ('C', 'Z', 1 + 6)
)
select sum(sr.SCORE) as RESULT
from day_02.INPUT p
join SCORE_RESULTS sr
  on p.PLAY = sr.PLAY
  and p.RESPONSE = sr.RESPONSE;