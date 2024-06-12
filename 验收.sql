# 验收

create database db0;
create database db1;
create database db2;
show databases;
use db0;
create table account(
  id int, 
  name char(16), 
  balance float, 
  primary key(id)
);
execfile "./sql_gen/account00.txt";
execfile "./sql_gen/account01.txt";
execfile "./sql_gen/account02.txt";
execfile "./sql_gen/account03.txt";
execfile "./sql_gen/account04.txt";
execfile "./sql_gen/account05.txt";
execfile "./sql_gen/account06.txt";
execfile "./sql_gen/account07.txt";
execfile "./sql_gen/account08.txt";
execfile "./sql_gen/account09.txt";

select * from account;
select * from account where name = "name56789"; # t1
select * from account where id < 12500200 and name < "name00100"; # t5
insert into account values(12500080, "name_0080", 100.2); # key already exists

show indexes;
create index idx01 on account(name);
show indexes;
select * from account where name = "name56789"; # t2, t2 < t1
select * from account where name = "name45678"; # t3
select * from account where id < 12500200 and name < "name00100"; # t6, t6 ? t5
delete from account where name = "name45678";
insert into account values(12114514, "name45678", 114.514);
drop index idx01;
show indexes;
select * from account where name = "name45678"; # t4, t3 < t4

update account set id = 12345678, balance = 123.4 where name = "name56789";
select * from account where name = "name56789";

delete from account where balance = 114.514;
select * from account where balance = 114.514;
delete from account;
select * from account;
drop table account;
show tables;


create database db0;
use db0;
create table account(
  id int, 
  name char(16), 
  balance float, 
  primary key(id)
);
drop table account;