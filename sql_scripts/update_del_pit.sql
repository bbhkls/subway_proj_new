with tab as (select ssc.valid_from_dttm, aac.client_rk, lag(valid_from_dttm) over(partition by aac.client_rk order by valid_from_dttm) l
	    from dbt_schema."GPR_RV_S_CLIENT" ssc 
		join dbt_schema."GPR_BV_A_CLIENT" aac on ssc.client_rk = aac.x_client_rk)

update dbt_schema."GPR_BV_P_{{params.dim}}"								
set (dataflow_id, dataflow_dttm, valid_to_dttm) = ('{{run_id}}', '{{execution_date}}'::timestamp, '{{execution_date}}'::timestamp)
where ((client_rk, 1, valid_from_dttm) in (select ac.client_rk a,  min(delete_flg),  
												  (select l from tab
												  where valid_from_dttm = sc.valid_from_dttm and client_rk = ac.client_rk and valid_from_dttm <> l)
										from dbt_schema."GPR_RV_S_{{params.dim}}" sc 
										join dbt_schema."GPR_BV_A_{{params.dim}}" ac on sc.client_rk = ac.x_client_rk
										where actual_flg = 1 and delete_flg = 1 
										group by ac.client_rk, valid_from_dttm
										having sc.valid_from_dttm = max(sc.valid_from_dttm))
   or client_rk not in (select client_rk from dbt_schema."GPR_BV_A_{{params.dim}}")) and extract(year from valid_to_dttm) = 5999;

commit;