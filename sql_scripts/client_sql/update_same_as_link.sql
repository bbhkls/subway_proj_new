UPDATE dbt_schema."GPR_BV_A_CLIENT" a_main
SET client_rk  =
	   (select 
			distinct coalesce (first_value(unif_key) over(partition by not_unif_key order by valid_from_dttm, unif_key), not_unif_key)
		from 
				(
				select 
					s2.client_rk not_unif_key
			  	  , s1.client_rk  unif_key
			  	  , s1.valid_from_dttm 
				from 
					dbt_schema."GPR_RV_S_CLIENT" s2
				left join 
				dbt_schema."GPR_RV_S_CLIENT" s1
			 	on s1.client_rk != s2.client_rk and s1.client_name_desc = s2.client_name_desc and s1.client_phone_desc = s2.client_phone_desc
			 	where s2.client_rk = a_main.x_client_rk and s2.actual_flg = 1
				)
			where not_unif_key = a_main.x_client_rk
			)
WHERE  a_main.x_client_rk in 
				(select 
					client_rk
				from 
					(
					select 
					  client_name_desc name 
					, client_phone_desc phone
					, lag(client_name_desc, 1, client_name_desc) over(partition by client_rk order by valid_from_dttm) old_name
					, lag(client_phone_desc, 1, client_phone_desc) over(partition by client_rk order by valid_from_dttm) old_phone
					, actual_flg 
					, valid_from_dttm
					, client_rk 
					from 
						dbt_schema."GPR_RV_S_CLIENT" 
					)
				where (name != old_name or phone != old_phone ) and actual_flg = 1);
	
commit;