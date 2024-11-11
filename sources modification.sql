-- new Source
-- CREATE
create table profile_card_post (
	id integer, 
	card_num bigint, 
	FIO varchar, 
	birthday date, 
	phone_num varchar, 
	service_name varchar, 
	discount integer,
	primary key(id)
);
  
-- DROP
drop table profile_card_post;
drop table receipt_post;

-- SELECT
select * from serps.profile_card_post;

-- DELETE
delete from serps.profile_card_post;

-- GRANT
GRANT ALL PRIVILEGES ON profile_card_post TO bl_gaia;
GRANT ALL PRIVILEGES ON profile_card_post TO mike;
GRANT ALL PRIVILEGES ON profile_card_post TO dbt_user;

-- INSERT
insert into serps.profile_card_post --(id, card_num, FIO, birthday, phone_num, service_name, discount)
select 1 id, 506686453 card_num, 'Зыкова Мария Игоревна' FIO, '12-03-1994'::date  birthday, '+79998348293' phone_num, 'Пятерочка' service_name, 15 discount
union all
select 2 id, 312841712 card_num, 'Конопатов Алексей Геннадьевич' FIO, '25-04-1993'::date  birthday, '+79249185726' phone_num, 'Пятерочка' service_name, 15 discount
union all
select 3 id, 285856520 card_num, 'Буров Артем Александрович' FIO, '02-11-2004'::date  birthday, '+79922847332' phone_num, 'Пятерочка' service_name, 15 discount
union all
select 4 id, 631626513 card_num, 'Буров Артем Александрович' FIO, '02-11-2004'::date  birthday, '+79482845932' phone_num, 'Пятерочка' service_name, 15 discount;
