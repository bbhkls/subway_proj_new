-- new Sources
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

create table receipt_post(
	id_operation integer, 
	id_seller integer, 
	id_buyer integer, 
	id_product integer, 
	id_product_connection integer, 
	id_disc_card bigint,
	sel_dttm timestamp, 
	cnt integer, 
	price float(2), 
	combo_group integer, 
	tovar_group integer,
	primary key(id_operation, id_seller, id_buyer, id_product, id_product_connection, sel_dttm)
 );
  
-- DROP
drop table profile_card_post;
drop table receipt_post;

-- SELECT
select * from serps.profile_card_post;
select * from serps.receipt_post;

-- DELETE
delete from serps.profile_card_post;
delete from serps.receipt_post;

-- GRANT
GRANT ALL PRIVILEGES ON profile_card_post TO bl_gaia;
GRANT ALL PRIVILEGES ON profile_card_post TO mike;
GRANT ALL PRIVILEGES ON profile_card_post TO dbt_user;

GRANT ALL PRIVILEGES ON receipt_post TO bl_gaia;
GRANT ALL PRIVILEGES ON receipt_post TO mike;
GRANT ALL PRIVILEGES ON receipt_post TO dbt_user;

-- INSERT
-- serps.profile_card_post
insert into serps.profile_card_post --(id, card_num, FIO, birthday, phone_num, service_name, discount)
select 1 id, 506686453 card_num, 'Зыкова Мария Игоревна' FIO, '12-03-1994'::date  birthday, '+79998348293' phone_num, 'Пятерочка' service_name, 15 discount
union all
select 2 id, 312841712 card_num, 'Конопатов Алексей Геннадьевич' FIO, '25-04-1993'::date  birthday, '+79249185726' phone_num, 'Пятерочка' service_name, 15 discount
union all
select 3 id, 285856520 card_num, 'Буров Артем Александрович' FIO, '02-11-2004'::date  birthday, '+79482845932' phone_num, 'Пятерочка' service_name, 15 discount
union all
select 4 id, 631626513 card_num, 'Буров Артем Александрович' FIO, '02-11-2004'::date  birthday, '+79482845932' phone_num, 'Пятерочка' service_name, 15 discount;

-- serps.receipt_post
insert into serps.receipt_post --(id_operation, id_seller, id_buyer, id_product, id_product_connection, id_disc_card, sel_dttm, cnt, price, combo_group, tovar_group)
select 1 id_operation, 1 id_seller, 1 id_buyer, 1 id_product, null id_product_connection, 
	   312841712 id_disc_card, '27-10-2024 14:20:31'::timestamp sel_dttm, 2 cnt, 50 price, null combo_group, 1 tovar_group
union all
select 1 id_operation, 1 id_seller, 1 id_buyer, 2 id_product, null id_product_connection, 
	   312841712 id_disc_card, '27-10-2024 14:20:31'::timestamp sel_dttm, 1 cnt, 100 price, 1 combo_group, 2 tovar_group
union all
select 1 id_operation, 1 id_seller, 1 id_buyer, 3 id_product, null id_product_connection, 
	   312841712 id_disc_card, '27-10-2024 14:20:31'::timestamp sel_dttm, 1 cnt, 200 price, 1 combo_group, 2 tovar_group
union all
select 2 id_operation, 1 id_seller, 2 id_buyer, 3 id_product, null id_product_connection, 
	   108671570 id_disc_card, '27-10-2024 14:41:23'::timestamp sel_dttm, 2 cnt, 200 price, null combo_group, 1 tovar_group
union all
select 2 id_operation, 1 id_seller, 1 id_buyer, 12 id_product, 3 id_product_connection, 
	   108671570 id_disc_card, '27-10-2024 14:41:23'::timestamp sel_dttm, -1 cnt, 20 price, null combo_group, 1 tovar_group
union all
select 3 id_operation, 5 id_seller, 10 id_buyer, 24 id_product, null id_product_connection, 
	   285856520 id_disc_card, '27-10-2024 20:21:15'::timestamp sel_dttm, 1 cnt, 250 price, null combo_group, 1 tovar_group