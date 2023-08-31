# Dockerfile for haredis
# Ex. Usage: docker build -t haredis-base:latest --build-arg PYTHON_VERSİON=3.8-bullseye -f ./include/Dockerfile .

ARG PYTHON_VERSİON=3.8-bullseye
FROM python:${PYTHON_VERSİON}

LABEL maintainer="Cevat Batuhan Tolon <cevat.batuhan.tolon@cern.ch>"

COPY . /opt/haredis
WORKDIR /opt/haredis
RUN pip install -e .

WORKDIR /