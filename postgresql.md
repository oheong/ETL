# postgresql



```mysql
select * from pg_shadow; 

create table member(
	no SERIAL primary key, -- mysql의 auto-increase == SERIAL
	name varchar(20) not null,
	mail varchar(30) not null,
	password varchar(30) not null,
	birth Date
);

insert into member(name, mail, password, birth)
values ('최승철','goodFace@dbcs.co.kr','1234','1995-8-8');
insert into member(name, mail, password, birth)
values ('전원우','nicdFace@dbcs.co.kr','1234','1996-5-26');

select * from member order by no;

```

테이블 만들기 완료~!