FROM philippeietesting/crypto-backend:latest

COPY --from=philippeietesting/crypto-backend:latest ./var/task/cg_db_utils/ .

COPY ./lambda_functions/coinmarketcap/coinmarketcap_daily_info_puller/cmc_puller.py .
COPY ./lambda_functions/coinmarketcap/coinmarketcap_daily_info_puller/supporting_functions.py .


CMD ["cmc_puller.cmc_lambda_handler"]