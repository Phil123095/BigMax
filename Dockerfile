FROM public.ecr.aws/lambda/python:3.8

COPY .env .

COPY ./cg_db_utils/ .

COPY ./requirements.txt .

RUN pip install -r requirements.txt

