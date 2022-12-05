drop schema if exists DAY_03 cascade;
create schema DAY_03;
drop table if exists TEMP_INPUT;

create temp table TEMP_INPUT (
  LINE_NUMBER serial,
  LINE        text
);

create table DAY_03.RUCKSACK (
  ID                 serial,
  FIRST_COMPARTMENT  text,
  SECOND_COMPARTMENT text
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY TEMP_INPUT(LINE) from 'sql/day03/input.txt' with (FORMAT 'text');

-- load RUCKSACK from temp table
insert into day_03.RUCKSACK (ID, FIRST_COMPARTMENT, SECOND_COMPARTMENT) 
select ti.LINE_NUMBER
     , substring(ti.LINE FROM 1 FOR length(ti.LINE) / 2)
     , substring(ti.LINE FROM (length(ti.LINE) / 2) + 1 FOR length(ti.LINE))
from TEMP_INPUT ti;

\echo 'First star:'
/* 
 * Find the item type that appears in both compartments of each rucksack.
 * What is the sum of the priorities of those item types?
 */
with priorities(letter, priority) as (
  select string_to_table('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', null)
       , generate_series(1, 52)
  ), 
  duplicated_items(id, item) as (
    select ID, i.ITEM
    from day_03.RUCKSACK r
    cross join string_to_table(r.FIRST_COMPARTMENT, null) as i(ITEM)
    intersect 
    select ID, i.ITEM
    from day_03.RUCKSACK r
    cross join string_to_table(r.SECOND_COMPARTMENT, null) as i(ITEM)
  )

select sum(p.PRIORITY)
from DUPLICATED_ITEMS di
join PRIORITIES p
  on di.ITEM = p.LETTER;


\echo 'Second star:'
/* 
 * Find the item type that corresponds to the badges of each three-Elf group.
 * What is the sum of the priorities of those item types?
 */
with priorities(letter, priority) as (
  select string_to_table('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', null)
       , generate_series(1, 52)
  ), 
  rucksacks(id, item, team, elf) as (
    select r.ID
         , i.ITEM
         , ((r.ID - 1) / 3) + 1 as team -- three by three
         , MOD(r.ID - 1, 3) + 1 as elf -- index of elf inside team
    from day_03.RUCKSACK r
    cross join string_to_table(r.FIRST_COMPARTMENT || r.SECOND_COMPARTMENT, null) as i(ITEM)
  ),
  teams(team, item) as (
    select r.TEAM, r.ITEM from RUCKSACKS r where r.ELF = 1 
    intersect 
    select r.TEAM, r.ITEM from RUCKSACKS r where r.ELF = 2 
    intersect
    select r.TEAM, r.ITEM from RUCKSACKS r where r.ELF = 3 
  )

select sum(p.PRIORITY) 
from teams t
join priorities p
  on t.ITEM = p.LETTER;