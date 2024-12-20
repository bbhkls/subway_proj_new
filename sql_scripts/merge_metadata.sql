MERGE INTO dbt_schema.metadata_airflow_test ma 
USING (select distinct oid source_n from {{params.param1}}) AS nt
ON ma.source_n = nt.source_n
WHEN MATCHED THEN
  UPDATE SET run_id = '{{run_id}}',
             execution_date = '{{ execution_date }}'
WHEN NOT MATCHED THEN
  INSERT (run_id, execution_date, source_n)
  VALUES ('{{run_id}}', '{{ execution_date }}', nt.source_n);
commit;