drop schema if exists DAY_04 cascade;
create schema DAY_04;

create table day_04.INPUT (
  LINE_NUMBER serial,
  LINE        text
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY day_04.INPUT(LINE) from 'sql/day04/input.txt' with (FORMAT 'text');

with split_sections as (
  select i.LINE_NUMBER
       , i.LINE
       , regexp_split_to_array(line, '[,-]')::int[] as sections
  from day_04.INPUT i
)
select  
      count(1) FILTER (where sections[1] <= sections[3] and sections[2] >= sections[4]
                          or sections[3] <= sections[1] and sections[4] >= sections[2] ) as first_star
    , count(1) FILTER (where sections[1] <= sections[4] and sections[2] >= sections[3]) as second_star
from split_sections;
