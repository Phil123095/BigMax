# Used as base for all lambda images.
######################################

FROM public.ecr.aws/lambda/python:3.8

COPY ./cg_db_utils .

COPY base-build/requirements.txt .

RUN pip install -r requirements.txt

