FROM python:latest

ADD . /tmp/pandaseq

RUN set -e \
      && ln -sf /bin/bash /bin/sh

RUN set -e \
      && apt-get -y update \
      && apt-get -y dist-upgrade \
      && apt-get -y autoremove \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

RUN set -e \
      && pip install -U --no-cache-dir pip /tmp/pandaseq \
      && rm -rf /tmp/pandaseq

ENTRYPOINT ["pandaseq"]
