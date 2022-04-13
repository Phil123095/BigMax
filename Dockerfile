################################################################
# IMPORTANT:
# WHEN BUILDING DOCKER FOR LIGHTSAIL ON LOCALMACHINE, RUN:
# docker buildx build --platform=linux/amd64 -t <image-name> .
################################################################

FROM python:3.8-slim

COPY base_resources/ .docke

RUN pip install -r requirements.txt
