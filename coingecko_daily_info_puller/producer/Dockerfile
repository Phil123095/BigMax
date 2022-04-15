FROM base-image-crypto-database:latest

COPY --from=base-image-crypto-database:latest ./var/task/cg_db_utils/ .

COPY ./coingecko_daily_info_puller/producer/main_with_class.py .
COPY ./coingecko_daily_info_puller/producer/mass_producer.py .

CMD ["main_with_class.cg_daily_pull_lambda"]