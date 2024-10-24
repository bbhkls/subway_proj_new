-- обновление конца периода в случае, когда экземпляр сущности не удален
update dbt_schema."GPR_BV_P_CLIENT" pc
set (dataflow_id, dataflow_dttm, valid_to_dttm) = (select dataflow_id, dataflow_dtt, ld - interval '1 minute'
					   from 
							(select client_rk, ma.run_id dataflow_id, ma.execution_date dataflow_dtt, valid_to_dttm,
									lead(valid_from_dttm, 1, valid_to_dttm) over (partition by client_rk, valid_to_dttm order by valid_from_dttm) ld
							   from dbt_schema."GPR_BV_P_CLIENT", (select * from dbt_schema.metadata_airflow_test where source_n = 'csv') ma)
							  where valid_to_dttm <> ld and pc.client_rk = client_rk)
where (client_rk, valid_from_dttm) in (select client_rk, valid_from_dttm
										from 
											(select client_rk, valid_from_dttm,
												   lead(valid_from_dttm, 1, valid_to_dttm) over (partition by client_rk, valid_to_dttm order by valid_from_dttm) ld
											from dbt_schema."GPR_BV_P_CLIENT")
											where valid_to_dttm <> ld);

commit;