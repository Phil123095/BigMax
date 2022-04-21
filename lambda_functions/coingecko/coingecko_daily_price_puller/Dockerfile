FROM philippeietesting/crypto-backend:latest

COPY --from=philippeietesting/crypto-backend:latest ./var/task/cg_db_utils/ .

COPY ./lambda_functions/coingecko/coingecko_daily_price_puller/daily_price_updater.py .

CMD ["daily_price_updater.lambda_handler_daily_prices"]
