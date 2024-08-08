drop table if exists datetime01;
create table datetime01 (
  c1 int,
  c2 varchar(8),
  c3 char(4),
  c4 int
);

insert into
  datetime01(c1, c2, c3, c4)
values
  (1, 'b', '1', null);

-- alter table datetime01 add column c5 datetime, algorithm=instant;
-- alter table datetime01 add column c5 datetime, algorithm=copy;
alter table datetime01 add column c5 datetime; -- default: algorithm=instant

insert into
  datetime01(c1, c2, c3, c4, c5)
values
  (2,  'bb',  '22', null, null),
  (3, 'bbb', '333', null, now());
