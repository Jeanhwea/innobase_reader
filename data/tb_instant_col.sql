-- on MySQL 8.0.27
drop table if exists tb_instant_col;

create table tb_instant_col (
  k1 int,
  c1 varchar(8),
  c2 varchar(8)
);

insert into tb_instant_col(k1, c1, c2) values (1, 'r1c1', 'r1c2');
insert into tb_instant_col(k1, c1, c2) values (2, 'r2c1', 'r2c2');

-- tb_instant_col_0.ibd
-- mysql> select * from tb_instant_col;
-- +------+------+------+
-- | k1   | c1   | c2   |
-- +------+------+------+
-- |    1 | r1c1 | r1c2 |
-- |    2 | r2c1 | r2c2 |
-- +------+------+------+
-- 2 rows in set (0.00 sec)

alter table tb_instant_col add column c3 varchar(8) default 'c3_def';

-- tb_instant_col_1.ibd
-- mysql> select * from tb_instant_col;
-- +------+------+------+--------+
-- | k1   | c1   | c2   | c3     |
-- +------+------+------+--------+
-- |    1 | r1c1 | r1c2 | c3_def |
-- |    2 | r2c1 | r2c2 | c3_def |
-- +------+------+------+--------+
-- 2 rows in set (0.00 sec)

insert into tb_instant_col(k1, c1, c2, c3) values (3, 'r3c1', 'r3c2', 'r3c3');

-- tb_instant_col_2.ibd
-- mysql> select * from tb_instant_col;
-- +------+------+------+--------+
-- | k1   | c1   | c2   | c3     |
-- +------+------+------+--------+
-- |    1 | r1c1 | r1c2 | c3_def |
-- |    2 | r2c1 | r2c2 | c3_def |
-- |    3 | r3c1 | r3c2 | r3c3   |
-- +------+------+------+--------+
-- 3 rows in set (0.00 sec)
