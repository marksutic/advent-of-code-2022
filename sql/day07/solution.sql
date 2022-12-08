drop schema if exists DAY_07 cascade;
create schema DAY_07;

create table DAY_07.INPUT (
  LINE_NUMBER serial,
  LINE        text
);
        
-- Use \COPY rather than COPY so its client-side in psql
\COPY day_07.INPUT(LINE) from 'sql/day07/input.txt' with (FORMAT 'text');

\echo 'First star:'
with recursive filesystem as (
     select '' as LINE
         , min(i.LINE_NUMBER) - 1 as ID
         , null::text[] as FULL_PATH_ARRAY --printing it out
         , '{}'::text[] as PATH
         , null::text as FILE_OR_FOLDER_NAME
         , null::bigint as SIZE
    from day_07.INPUT i
    union all
    select 
          i.LINE
        , i.LINE_NUMBER as ID
        , parts --printing it out
        , case when parts[1] = '$' and parts[2] = 'cd' 
               then 
                    case when parts[3] = '..'
                         then trim_array(PATH, 1) --remove last folder from path
                         else array_append(PATH, parts[3]) --append folder to path
                    end
               else PATH 
          end
        , case when parts[1] != '$' then parts[2] end
        , case when parts[1] NOT IN ('$', 'dir') then parts[1]::bigint end
    from filesystem
    join day_07.INPUT i
      on filesystem.ID + 1 = i.LINE_NUMBER
    cross join lateral string_to_array(i.LINE, ' ') as _(parts)
) 
select sum(SIZE_PER_DIR)
from (
  select PATH_, sum(f.SIZE) as SIZE_PER_DIR
  from filesystem f
  cross join lateral generate_series(1, array_length(PATH, 1)) as _(ARR_LEN)
  cross join lateral trim_array(PATH, ARR_LEN - 1) as trimmed(PATH_)
  where f.SIZE is not null
  group by PATH_
  having sum(f.SIZE) <= 100000 
) _
;


\echo 'Second star:'
with recursive filesystem as (
     select '' as LINE
         , min(i.LINE_NUMBER) - 1 as ID
         , null::text[] as FULL_PATH_ARRAY -- printing it out
         , '{}'::text[] as PATH
         , null::text as FILE_OR_FOLDER_NAME
         , null::bigint as SIZE
    from day_07.INPUT i
    union all
    select 
          i.LINE
        , i.LINE_NUMBER as ID
        , parts -- printing it out
        , case when parts[1] = '$' and parts[2] = 'cd' 
               then 
                    case when parts[3] = '..'
                         then trim_array(PATH, 1) --remove last folder from path
                         else array_append(PATH, parts[3]) --append folder to path
                    end
               else PATH 
          end
        , case when parts[1] != '$' then parts[2] end
        , case when parts[1] NOT IN ('$', 'dir') then parts[1]::bigint end
    from filesystem
    join day_07.INPUT i
      on filesystem.ID + 1 = i.LINE_NUMBER
    cross join lateral string_to_array(i.LINE, ' ') as _(parts)
) 
select SIZE_PER_DIR
from (
  select PATH_
       , sum(f.SIZE) as SIZE_PER_DIR
       , max(sum(size)) OVER () - sum(size) AS SIZE_AFTER_DELETE
  from filesystem f
  cross join lateral generate_series(1, array_length(PATH, 1)) as _(ARR_LEN)
  cross join lateral trim_array(PATH, ARR_LEN - 1) as trimmed(PATH_)
  where f.SIZE is not null
  group by PATH_
) _
where _.SIZE_AFTER_DELETE > 0 
  and _.SIZE_AFTER_DELETE <= 40000000
order by _.SIZE_AFTER_DELETE desc
fetch first row only
;