drop schema if exists DAY_01 cascade;
create schema DAY_01;
drop table if exists TEMP_INPUT;

create temp table TEMP_INPUT (
  ID    serial,
  VALUE text
);

create table day_01.INPUT (
  ID    serial,
  VALUE bigint
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY TEMP_INPUT(VALUE) from 'sql/day01/input.txt' with (FORMAT 'text');

insert into day_01.INPUT (ID, VALUE) select ti.ID, nullif(ti.VALUE, '')::bigint from TEMP_INPUT ti;

/* 
 * first star
 * find the Elf carrying the most Calories. 
 * How many total Calories is that Elf carrying?
 */
with CALORIES_PER_ELF as (
  select value as CALORIES
       , count(*) FILTER (where value is null) over(order by id) + 1 as ELF
       --, sum(case when value is null then 1 else 0 end) over(order by id) as group
  from day_01.INPUT
)

select ELF, sum(CALORIES) as TOTAL_CALORIES
from CALORIES_PER_ELF
group by ELF
order by TOTAL_CALORIES desc
fetch first row only;

/* 
 * second star 
 * find the top three Elves carrying the most Calories. 
 How many Calories are those Elves carrying in total?
 */
with CALORIES_PER_ELF as (
  select value as CALORIES
       , count(*) FILTER (where value is null) over(order by id) + 1 as ELF
  from day_01.INPUT
)
select sum(TOTAL_CALORIES) as TOP_THREE_ELVES_CALORIES
from (
  select ELF, sum(CALORIES) as TOTAL_CALORIES
  from CALORIES_PER_ELF
  group by ELF
  order by TOTAL_CALORIES desc
  fetch first 3 rows only
) top3