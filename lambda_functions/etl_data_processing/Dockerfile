FROM philippeietesting/crypto-backend:latest

COPY --from=philippeietesting/crypto-backend:latest ./var/task/cg_db_utils/ .

COPY ./lambda_functions/etl_data_processing/etl_dwh_process.py .
COPY ./lambda_functions/etl_data_processing/etl_dwh_tables.py .


CMD ["etl_dwh_process.etl_db_table_create"]