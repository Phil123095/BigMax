FROM philippeietesting/crypto-backend:latest

COPY --from=philippeietesting/crypto-backend:latest ./var/task/cg_db_utils/ .

COPY ./lambda_functions/coingecko/coingecko_daily_info_puller/consumer/lambda_handler.py .
COPY ./lambda_functions/coingecko/coingecko_daily_info_puller/consumer/coin_cleaner.py .
COPY ./lambda_functions/coingecko/coingecko_daily_info_puller/consumer/process_to_db.py .

CMD ["lambda_handler.lambda_handler"]