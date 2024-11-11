-- Reciep and CLient dim
-- CREATE
create table receipt_post(
	id_operation integer, 
	id_seller integer, 
	id_buyer integer, 
	id_product integer, 
	id_product_connection integer, 
	id_disc_card integer,
	sel_dttm timestamp, 
	cnt integer, 
	price float(2), 
	combo_group integer, 
	tovar_group integer);
  

CREATE TABLE CLIENT_FROM_STAR (
ID integer, 
NAME VARCHAR2(300), 
PHONE VARCHAR2(15), 
CITY VARCHAR2(20), 
BIRTHDAY DATE, 
AGE integer
);
 
-- DROP
drop table receipt_post;
drop TABLE CLIENT_FROM_STAR;

-- SELECT
select * from serps.receipt_post;
SELECT * FROM serps.CLIENT_FROM_STAR;

-- DELETE
delete from serps.receipt_post;
DELETE FROM serps.CLIENT_FROM_STAR;

-- GRANT
GRANT ALL PRIVILEGES ON receipt_post TO bl_gaia;
GRANT ALL PRIVILEGES ON receipt_post TO mike;
GRANT ALL PRIVILEGES ON receipt_post TO alexeymn;

GRANT ALL PRIVILEGES ON CLIENT_FROM_STAR TO bl_gaia;
GRANT ALL PRIVILEGES ON CLIENT_FROM_STAR TO mike;
GRANT ALL PRIVILEGES ON CLIENT_FROM_STAR TO alexeymn;

-- INSERT
-- serps.receipt_post
insert into serps.receipt_post --(id_operation, id_seller, id_buyer, id_product, id_product_connection, id_disc_card, sel_dttm, cnt, price, combo_group, tovar_group)
select 1 id_operation, 1 id_seller, 1 id_buyer, 1 id_product, -1 id_product_connection, 
	   312841712 id_disc_card, TO_TIMESTAMP('27-10-2024 14:20:31', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, 2 cnt, 50 price, 0 combo_group, 1 tovar_group
FROM dual
UNION all
select 1 id_operation, 1 id_seller, 1 id_buyer, 2 id_product, -1 id_product_connection, 
	   312841712 id_disc_card, TO_TIMESTAMP('27-10-2024 14:20:31', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, 1 cnt, 100 price, 1 combo_group, 2 tovar_group
FROM dual
UNION ALL
select 1 id_operation, 1 id_seller, 1 id_buyer, 3 id_product, -1 id_product_connection, 
	   312841712 id_disc_card, TO_TIMESTAMP('27-10-2024 14:20:31', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, 1 cnt, 200 price, 1 combo_group, 2 tovar_group
FROM dual
UNION ALL
select 2 id_operation, 1 id_seller, 2 id_buyer, 3 id_product, -1 id_product_connection, 
	   108671570 id_disc_card, TO_TIMESTAMP('27-10-2024 14:41:23', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, 2 cnt, 200 price, 0 combo_group, 1 tovar_group
FROM dual
UNION ALL
select 2 id_operation, 1 id_seller, 2 id_buyer, 12 id_product, 3 id_product_connection, 
	   108671570 id_disc_card, TO_TIMESTAMP('27-10-2024 14:41:23', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, -1 cnt, 20 price, 0 combo_group, 1 tovar_group
FROM dual
UNION all
select 3 id_operation, 5 id_seller, 5 id_buyer, 24 id_product, -1 id_product_connection, 
	   285856520 id_disc_card, TO_TIMESTAMP('28-10-2024 20:21:15', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, 1 cnt, 250 price, 0 combo_group, 1 tovar_group
FROM dual;

-- serps.CLIENT_FROM_STAR
INSERT INTO serps.client_from_star
SELECT 1 id, 'Буров Артем Александрович' name, '+79922847332' phone, 'Москва' city, to_date('02-11-2004', 'dd-mm-yyyy') birthday, 2024 - 2004 age 
FROM dual
union
SELECT 2 id, 'Зыкова Мария Игоревна' name, '+79998348293' phone, 'Петербург' city, to_date('12-03-1994', 'dd-mm-yyyy') birthday, 2024 - 1994 age 
FROM dual
UNION
SELECT 3 id, 'Зыкова Мария Игоревна' name, '+79998196351' phone, 'Петербург' city, to_date('12-03-1994', 'dd-mm-yyyy') birthday, 2024 - 1994 age 
FROM dual
UNION
SELECT 4 id, 'Чапаенко Кирилл Николаевич' name, '+70278263740' phone, 'Воронеж' city, to_date('25-06-2001', 'dd-mm-yyyy') birthday, 2024 - 2001 age 
FROM dual
UNION
SELECT 5 id, 'Коноваев Петр Артемович' name, '+73571937482' phone, 'Караганда' city, to_date('09-02-1999', 'dd-mm-yyyy') birthday, 2024 - 1999 age 
FROM dual;