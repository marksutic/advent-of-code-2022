drop schema if exists DAY_06 cascade;
create schema DAY_06;

create table DAY_06.INPUT (
  LINE        text
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY day_06.INPUT(LINE) from 'sql/day06/input.txt' with (FORMAT 'text');

\echo 'First star:'
with unique_length (param_value) as (
  select 4
)
select idx 
from (
    select idx
         , line 
         , substring(i.line, idx, 1) AS CHAR_AT_IDX
         , substring(i.line, idx - 1, 1) as CHAR_AT_IDX_MINUS_1
         , substring(i.line, idx - 2, 1) as CHAR_AT_IDX_MINUS_2
         , substring(i.line, idx - 3, 1) as CHAR_AT_IDX_MINUS_3
         , substring(i.line, idx - 3, (select param_value from unique_length)) AS XXXXDX
         , (select count(*) from (select distinct unnest(string_to_array( substring(i.line, 
                                                                                    idx - ((select param_value - 1 from unique_length)), 
                                                                                    (select param_value from unique_length)
                                                                                    ), null)))_ ) as UNIQUE_LEN
    from day_06.INPUT i
    cross join lateral generate_series((select param_value from unique_length), length(i.LINE)) series(idx)
    order by IDX 
) _
where _.unique_len = (select param_value from unique_length)
fetch first row only
;

-- the queries are the same, unique length is parameterized
\echo 'Second star:'
with unique_length (param_value) as (
  select 14
)
select idx 
from (
    select idx
         , line 
         , substring(i.line, idx, 1) AS CHAR_AT_IDX
         , substring(i.line, idx - 1, 1) as CHAR_AT_IDX_MINUS_1
         , substring(i.line, idx - 2, 1) as CHAR_AT_IDX_MINUS_2
         , substring(i.line, idx - 3, 1) as CHAR_AT_IDX_MINUS_3
         , substring(i.line, idx - 3, (select param_value from unique_length)) AS XXXXDX
         , (select count(*) from (select distinct unnest(string_to_array( substring(i.line, 
                                                                                    idx - ((select param_value - 1 from unique_length)), 
                                                                                    (select param_value from unique_length)
                                                                                    ), null)))_ ) as UNIQUE_LEN
    from day_06.INPUT i
    cross join lateral generate_series((select param_value from unique_length), length(i.LINE)) series(idx)
    order by IDX 
) _
where _.unique_len = (select param_value from unique_length)
fetch first row only
;