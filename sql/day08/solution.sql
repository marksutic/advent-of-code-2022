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
;

\echo 'Second star:'
with tree_matrix as (
  select 
         line
       , height::int
       , x - 1 as column_
       , line_number - 1 as row_
  from day_08.INPUT i
  cross join lateral string_to_table(i.LINE, null) with ordinality as _(height, x)
), scores as (
  select 
         column_
       , row_
       , height 
       -- array agg does not preserve ordering of values so we use jsonb_object_agg
       --, array_agg(height) OVER (partition by column_ order by row_ asc  rows unbounded preceding exclude current row) as north_array
       , jsonb_object_agg(row_, height) OVER (partition by column_ order by row_ asc rows unbounded preceding exclude current row) as north
       , jsonb_object_agg(row_, height) OVER (partition by column_ order by row_ desc rows unbounded preceding exclude current row) as south
       , jsonb_object_agg(column_, height) OVER (partition by row_ order by column_ asc rows unbounded preceding exclude current row) as west
       , jsonb_object_agg(column_, height) OVER (partition by row_ order by column_ desc rows unbounded preceding exclude current row) as east
  from tree_matrix
)
select max(POINTS_NORTH * POINTS_WEST * POINTS_SOUTH * POINTS_EAST)
from (
    select 
             column_
           , row_
           , height
           , north
           , south
           , west
           , east
           --, row_ - (select max(k::int) filter (where height <= v::int) from jsonb_each_text(north) as n(k, v)) as POINTS_NORTH --nulls?
           , row_ - (select max(case when height <= v::int then k::int else 0 end) from jsonb_each_text(north) as n(k, v) ) as POINTS_NORTH
           , column_ - (select max(case when height <= v::int then k::int else 0 end) from jsonb_each_text(west) as w(k, v) ) as POINTS_WEST
           , (select coalesce(min(k::bigint) filter(where height <= v::int), max(k::bigint)) FROM jsonb_each(south) as s(k, v)) - row_ as POINTS_SOUTH
           , (select coalesce(min(k::bigint) filter(where height <= v::int), max(k::bigint)) FROM jsonb_each(east) as e(k, v)) - column_ as POINTS_EAST
    from scores
    where north is not null 
      and south is not null
      and west is not null 
      and east is not null
) _
;