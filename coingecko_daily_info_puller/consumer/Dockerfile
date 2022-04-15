FROM base-image-crypto-database:latest

COPY --from=base-image-crypto-database:latest ./var/task/cg_db_utils/ .

COPY ./coingecko_daily_info_puller/consumer/lambda_handler.py .
COPY ./coingecko_daily_info_puller/consumer/coin_cleaner.py .
COPY ./coingecko_daily_info_puller/consumer/process_to_db.py .

CMD ["lambda_handler.lambda_handler"]