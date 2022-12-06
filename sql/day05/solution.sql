drop schema if exists DAY_05 cascade;
create schema DAY_05;

create table DAY_05.INPUT (
  LINE_NUMBER serial,
  LINE        text
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY day_05.INPUT(LINE) from 'sql/day05/input.txt' with (FORMAT 'text');

\echo 'First star:'
/*
 * https://stackoverflow.com/questions/35248217/how-to-use-multiple-ctes-in-a-single-sql-query
 * Use the key word WITH once at the top. If any of your Common Table Expressions (CTE) are recursive (rCTE) you have to add the keyword 
 * RECURSIVE at the top once also, even if not all CTEs are recursive:
*/
with recursive stacks as (
    select STACK_ID
         , string_agg(PART, '' order by LINE_NUMBER asc) filter (where STACK_ID != PART) as ELEMENTS
    from ( select line_number
                , part
                , col_idx
                , last_value(part) over (partition by COL_IDX order by LINE_NUMBER range between unbounded preceding and unbounded following) as stack_id
           from day_05.INPUT
           cross join string_to_table(line, null) WITH ORDINALITY AS parts(PART, COL_IDX)
           where line not like '%move%from%to%') _ 
     where _.STACK_ID not in (' ', '') 
       and _.PART not in (' ', '')
     group by _.STACK_ID
), moves as (
    select _.MOVE_ARRAY[1] as amount
         , _.MOVE_ARRAY[2] as stack_pop
         , _.MOVE_ARRAY[3] as stack_push
         , rank() over (order by _.LINE_NUMBER) AS ID
    from (
      select i.LINE_NUMBER
           , string_to_array(replace(replace(replace(i.LINE, ' to ' , '|'), ' from ' , '|'), 'move ' , ''), '|')::int[] as MOVE_ARRAY
      from day_05.INPUT i
      where i.LINE like '%move%from%to%' ) _
), walk as (
    select 1 as MOVE_ID
         , jsonb_object_agg(STACK_ID, ELEMENTS) as STACKS --https://stackoverflow.com/questions/39922315/build-json-from-2-aggregated-columns-in-postgres
    from STACKS
    union all
    select walk.MOVE_ID + 1
         , game.STACKS
    from WALK
    join MOVES m 
      on m.ID = MOVE_ID
    cross join lateral ( select jsonb_object_agg --https://stackoverflow.com/questions/26107915/call-a-set-returning-function-with-an-array-argument-multiple-times/26514968#26514968
                                ( key
                                , case when KEY::int = STACK_POP  then substring(value, AMOUNT + 1)
                                       when KEY::int = STACK_PUSH then reverse(substring(STACKS ->> STACK_POP::text, 1, AMOUNT)) || VALUE
                                  else value end )
                         from jsonb_each_text(STACKS)
                        ) game(STACKS)
)

select 
  --key, value,
  string_agg(substring(value, 1, 1), '' ORDER BY key) as CRATES
from (
  select stacks 
  from walk
  order by move_id desc
  fetch first row only
) _1
cross join jsonb_each_text(_1.stacks)
--group by key, value
