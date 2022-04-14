FROM base-image-crypto-database:latest

COPY --from=base-image-crypto-database:latest ./var/task/cg_db_utils/ .

COPY ./coingecko_daily_price_puller/daily_price_updater.py .

CMD ["daily_price_updater.lambda_handler_daily_prices"]
