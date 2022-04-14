FROM python:3.8-slim

COPY .env .

COPY ./cg_db_utils/ .

COPY ./requirements.txt .

COPY ./coingecko_daily_price_puller/daily_price_updater.py .

RUN pip install -r requirements.txt


CMD ["python", "./daily_price_updater.py"]
