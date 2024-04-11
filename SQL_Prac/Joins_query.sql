create database joinsdb;

use joinsdb;

create table employee(
 emp_id int,
 emp_name varchar(20),
 salary int,
 dept_id varchar(20),
 manager_id varchar(20))

 ALTER TABLE employee
ALTER COLUMN emp_id varchar(20);

select * from employee;

insert into employee values('E1','Rahul',15000,'D1','M1');
insert into employee values('E2','Manoj',15000,'D1','M1');
insert into employee values('E3','James',55000,'D2','M2');
insert into employee values('E4','Michael',25000,'D3','M2');
insert into employee values('E5','Ali',20000,'D7','M3');

create table department(
  dept_id varchar(20),
  dept_name varchar(20));

insert into department values ('D1','IT');
insert into department values ('D2','HR');
insert into department values ('D3','Finance');
insert into department values ('D4','Admin');

select * from department;

create table manager(
 manager_id varchar(20),
 manager_name varchar(20),
 dept_id varchar(20));

insert into manager values('M1','Prem','D3');
insert into manager values('M2','Shripadh','D4');
insert into manager values('M3','Nick','D1');
insert into manager values('M4','Cory','D1');

select * from manager;

create table project(
project_id varchar(20),
project_name varchar(20),
team_member_id varchar(20));

insert into project values('P1','Data Migration','E1');
insert into project values('P1','Data Migration','M1');
insert into project values('P2','ETL Tool','E2');
insert into project values('P2','ETL Tool','M3');

select * from project;

--fetch the employee name and department name they belong to.
--INNER JOIN/ JOIN : it is used to retrive macthing records from both the tables 
select e.emp_name, d.dept_name
from employee e
join department d on e.dept_id = d.dept_id;

--fetch all the employee name and their department name.
--LEFT JOIN : it is used retrive matching records from both the table and unmatching records from the left table
select e.emp_name,d.dept_name
from employee e
left join department d on e.dept_id = d.dept_id;

--RIGHT JOIN : it is used retrive matching records from both the table and unmatching records from the right table
select e.emp_name,d.dept_name
from employee e
right join department d on e.dept_id = d.dept_id;

--Full outer join/ full join :it is used retrive matching and unmatching records from both the tables
select e.emp_name,d.dept_name
from employee e
full outer join department d on e.dept_id = d.dept_id;

--cross join:- it is used to return cartesian product (it is used to merge one table with another table)
select e.emp_name,d.dept_name
from employee e
cross join department d;

--fetch all the employees, their department, their manager, their projects they work on.
select e.emp_name, d.dept_name, m.manager_name, p.project_name
from employee e
left join department d on e.dept_id = d.dept_id
inner join manager m on m.manager_id=e.manager_id
left join project p on p.team_member_id= e.emp_id

create table company(
 company_id varchar(20),
 company_name varchar(30),
 comp_location varchar(20))

insert into company values('C001','Diggibyte','Bengaluru')

select * from company;

--write a query to fetch the employee name and their corresponding department name.
--also make sure to display the company name and the company location corresponding to each employee

select e.emp_name, d.dept_name, c.company_name, c.comp_location
from employee e
join department d on e.dept_id = d.dept_id
cross join company c

--self join

create table family(
member_id varchar(20),
names varchar(20),
age int,
parent_id varchar(10));

insert into family values('F1','David',5,'F5');
insert into family values('F2','Carol',10,'F5');
insert into family values('F3','Michael',12,'F5');
insert into family values('F4','Johnson',36,null);
insert into family values('F5','Maryam',40,'F6');
insert into family values('F6','Stewart',70,null);
insert into family values('F7','Robin',6,'F4');
insert into family values('F8','Asha',8,'F4');


select * from family;

--write a query to fetch child name and their age corresponding to their parent name and parent age
--Self join : It is used to join the table itself
select child.names as child_name,
child.age as child_age,
parent.names as parent_name, 
parent.age as parent_age
from family as child
join family as parent on child.parent_id = parent.member_id;