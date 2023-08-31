FROM haredis-base:latest
LABEL maintainer="Cevat Batuhan Tolon <cevat.batuhan.tolon@cern.ch>"

ARG API_VOL

# Set API Volume For Monitoring Docker Process Later
ENV API_VOL=${API_VOL}

# Install some useful system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      curl \
      bash \
      vim \
      nano \
      unzip \
      rsync \
      coreutils \
      procps \
      unzip \
      software-properties-common \
      ssh \
      netcat \
      gcc \
      wget

# Clean up
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements.txt to the docker image
COPY ./requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt \
    && rm -rf /root/.cache/pip

# Copy some Gunicorn WSGI Server conf scripts to the docker image for production
COPY ./include/conf/gunicorn/gunicorn_conf.py /
COPY ./include/conf/gunicorn/start.sh /${API_VOL}/

# Copy codes to the docker image for CI/CD
COPY ../ /${API_VOL}/

# Set Workdir and PYTHONPATH as /app
ENV PYTHONPATH=/${API_VOL}
WORKDIR /${API_VOL}