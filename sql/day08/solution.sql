drop schema if exists DAY_08 cascade;
create schema DAY_08;

create table DAY_08.INPUT (
  LINE_NUMBER serial,
  LINE        text
);
        
-- Use \COPY rather than COPY so its client-side in psql
\COPY DAY_08.INPUT(LINE) from 'sql/day08/input.txt' with (FORMAT 'text');

\echo 'First star:'
with tree_matrix as (
  select 
         line
       , height::int
       , x - 1 as column_
       , line_number - 1 as row_
  from day_08.INPUT i
  cross join lateral string_to_table(i.LINE, null) with ordinality as _(height, x)
) 
select count(*)
from (
  select 
         column_
       , row_
       , height 
       , coalesce(max(height) OVER (partition by column_ order by row_    asc  rows unbounded preceding exclude current row), -1) as north
       , coalesce(max(height) OVER (partition by column_ order by row_    desc rows unbounded preceding exclude current row), -1) as south
       , coalesce(max(height) OVER (partition by row_    order by column_ asc  rows unbounded preceding exclude current row), -1) as west
       , coalesce(max(height) OVER (partition by row_    order by column_ desc rows unbounded preceding exclude current row), -1) as east
  from tree_matrix ) _
where _.height > least(north, east, south, west)  -- at least one has to lower than height