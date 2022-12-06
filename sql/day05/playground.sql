with data(color_id, language, name) as (
values
    (1, 'de', 'blau'),
    (1, 'en', 'blue'),
    (1, 'fr', 'bleu')
)
select color_id, (jsonb_object_agg(language, name))
from data
group by 1;

with recursive fun as (
  select 1 as n
       , 1::bigint as a
  from 
  union all
  select n + 1
       , a + n
  from fun
)
select * 
from fun
fetch first 3 rows only;

select *
from jsonb_each_text('{"de": "blau", "en": "blue", "fr": "bleu"}')
cross join lateral (
      select jsonb_object_agg( key, case when key = 'de' then 'Gut' else 'Scheiße' end)
      from jsonb_each_text('{"de": "blau", "en": "blue", "fr": "bleu"}')
) cjl ;


select '{"1": "NZ", "2": "DCM", "3": "P"}'::json->> '2'