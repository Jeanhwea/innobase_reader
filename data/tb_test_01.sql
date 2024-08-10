drop table if exists test01;
create table test01 (
  k1 int,
  c1 char(4),
  c2 char(4),
  primary key(k1)
);

insert into test01(k1, c1, c2) values (1, 'r1c1', 'r1c2');

alter table test01 add column d1 datetime;
alter table test01 add column d2 datetime default current_timestamp;
