create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
insert into pk_counts (total) select count(1) from T1
insert into fk_counts (total) select count(1) from T1
insert into pk_counts (dist) select count(1) from (select distinct K11 from T1 where (K11) is not null) as primary_count
insert into qm_table (tablename, entityerr, referentialerr, ok) values ('T1', (1.0 - (select min(dist)/ max(total) from pk_counts)), (0.0), 'N')
update qm_table set ok = 'Y' where entityerr <= 0.0 and referentialerr <= 0.0
drop table pk_counts
drop table fk_counts
create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
insert into pk_counts (total) select count(1) from T2
insert into fk_counts (total) select count(1) from T2
insert into pk_counts (dist) select count(1) from (select distinct K21,K22 from T2 where (K21,K22) is not null) as primary_count
insert into qm_table (tablename, entityerr, referentialerr, ok) values ('T2', (1.0 - (select min(dist)/ max(total) from pk_counts)), (0.0), 'N')
update qm_table set ok = 'Y' where entityerr <= 0.0 and referentialerr <= 0.0
drop table pk_counts
drop table fk_counts
create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
insert into pk_counts (total) select count(1) from T3
insert into fk_counts (total) select count(1) from T3
insert into pk_counts (dist) select count(1) from (select distinct K31 from T3 where (K31) is not null) as primary_count
insert into fk_counts (dist) select count(*) from T3 where K11 not in (select K11 from T1) or K11 is null
insert into qm_table (tablename, entityerr, referentialerr, ok) values ('T3', (1.0 - (select min(dist)/ max(total) from pk_counts)), (select min(dist)/ max(total) from fk_counts), 'N')
update qm_table set ok = 'Y' where entityerr <= 0.0 and referentialerr <= 0.0
drop table pk_counts
drop table fk_counts
create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
insert into pk_counts (total) select count(1) from T4
insert into fk_counts (total) select count(1) from T4
insert into pk_counts (dist) select count(1) from (select distinct K41,K42 from T4 where (K41,K42) is not null) as primary_count
insert into fk_counts (dist) select count(*) from T4 where K31 not in (select K31 from T3) or K31 is null
insert into qm_table (tablename, entityerr, referentialerr, ok) values ('T4', (1.0 - (select min(dist)/ max(total) from pk_counts)), (select min(dist)/ max(total) from fk_counts), 'N')
update qm_table set ok = 'Y' where entityerr <= 0.0 and referentialerr <= 0.0
drop table pk_counts
drop table fk_counts
create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))
insert into pk_counts (total) select count(1) from T5
insert into fk_counts (total) select count(1) from T5
insert into pk_counts (dist) select count(1) from (select distinct K51,K52,K53 from T5 where (K51,K52,K53) is not null) as primary_count
insert into qm_table (tablename, entityerr, referentialerr, ok) values ('T5', (1.0 - (select min(dist)/ max(total) from pk_counts)), (0.0), 'N')
update qm_table set ok = 'Y' where entityerr <= 0.0 and referentialerr <= 0.0
drop table pk_counts
drop table fk_counts
