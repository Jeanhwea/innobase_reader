drop table if exists tb_row_version;
create table tb_row_version (
  c1 char(10),
  c2 char(10),
  c3 char(10),
  c4 char(10)
);

insert into
  tb_row_version(c1, c2, c3, c4)
values
  ('r1c1', 'r1c2', 'r1c3', 'r1c4');

-- tb_row_version_0.ibd
-- mysql> select * from tb_row_version;
-- +------+------+------+------+
-- | c1   | c2   | c3   | c4   |
-- +------+------+------+------+
-- | r1c1 | r1c2 | r1c3 | r1c4 |
-- +------+------+------+------+
-- 1 row in set (0.00 sec)

alter table tb_row_version add column c5 char(10) default 'c5_def', algorithm=instant;

-- tb_row_version_1.ibd
-- mysql> select * from tb_row_version;
-- +------+------+------+------+--------+
-- | c1   | c2   | c3   | c4   | c5     |
-- +------+------+------+------+--------+
-- | r1c1 | r1c2 | r1c3 | r1c4 | c5_def |
-- +------+------+------+------+--------+
-- 1 row in set (0.00 sec)

insert into
  tb_row_version(c1, c2, c3, c4, c5)
values
  ('r2c1', 'r2c2', 'r2c3', 'r2c4', 'r2c5');

-- tb_row_version_2.ibd
-- mysql> select * from tb_row_version;
-- +------+------+------+------+--------+
-- | c1   | c2   | c3   | c4   | c5     |
-- +------+------+------+------+--------+
-- | r1c1 | r1c2 | r1c3 | r1c4 | c5_def |
-- | r2c1 | r2c2 | r2c3 | r2c4 | r2c5   |
-- +------+------+------+------+--------+
-- 2 rows in set (0.00 sec)

alter table tb_row_version drop column c3, algorithm=instant;

-- tb_row_version_3.ibd
-- mysql> select * from tb_row_version;
-- +------+------+------+--------+
-- | c1   | c2   | c4   | c5     |
-- +------+------+------+--------+
-- | r1c1 | r1c2 | r1c4 | c5_def |
-- | r2c1 | r2c2 | r2c4 | r2c5   |
-- +------+------+------+--------+
-- 2 rows in set (0.00 sec)

-- alter table tb_row_version add column c5 char(10), algorithm=instant;
-- alter table tb_row_version add column c5 char(10), algorithm=copy;
-- alter table tb_row_version add column c5 char(10), algorithm=inplace;
-- alter table tb_row_version add column c5 char(10); -- default: algorithm=instant
