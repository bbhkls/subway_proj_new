UPDATE dbt_schema.metadata_airflow SET run_id = '{{ run_id }}', execution_date = '{{ execution_date }}';
commit;