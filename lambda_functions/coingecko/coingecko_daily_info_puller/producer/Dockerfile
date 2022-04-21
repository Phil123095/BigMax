FROM philippeietesting/crypto-backend:latest

COPY --from=philippeietesting/crypto-backend:latest ./var/task/cg_db_utils/ .

COPY ./lambda_functions/coingecko/coingecko_daily_info_puller/producer/main_with_class.py .

CMD ["main_with_class.cg_daily_pull_lambda"]