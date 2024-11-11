-- Reciep
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
  
-- DROP
drop table receipt_post;

-- SELECT
select * from serps.receipt_post;

-- DELETE
delete from serps.receipt_post;

-- GRANT
GRANT ALL PRIVILEGES ON receipt_post TO bl_gaia;
GRANT ALL PRIVILEGES ON receipt_post TO mike;
GRANT ALL PRIVILEGES ON receipt_post TO alexeymn;

-- INSERT
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
select 2 id_operation, 1 id_seller, 1 id_buyer, 12 id_product, 3 id_product_connection, 
	   108671570 id_disc_card, TO_TIMESTAMP('27-10-2024 14:41:23', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, -1 cnt, 20 price, 0 combo_group, 1 tovar_group
FROM dual
UNION all
select 3 id_operation, 5 id_seller, 10 id_buyer, 24 id_product, -1 id_product_connection, 
	   285856520 id_disc_card, TO_TIMESTAMP('28-10-2024 20:21:15', 'dd-mm-yyyy hh24:mi:ss' ) sel_dttm, 1 cnt, 250 price, 0 combo_group, 1 tovar_group
FROM dual;