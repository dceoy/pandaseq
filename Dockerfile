FROM dceoy/jupyter:latest

ADD https://raw.githubusercontent.com/dceoy/print-github-tags/master/print-github-tags /usr/local/bin/print-github-tags
ADD . /tmp/pandna

RUN set -e \
      && apt-get -y update \
      && apt-get -y dist-upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        autoconf curl libbz2-dev libcurl4-gnutls-dev liblzma-dev libncurses5-dev libssl-dev wget \
        zlib1g-dev \
      && apt-get -y autoremove \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

RUN set -e \
      && chmod +x /usr/local/bin/print-github-tags \
      && print-github-tags --release --latest --tar samtools/htslib \
        | xargs -i curl -SL {} -o /tmp/htslib.tar.gz \
      && tar xvf /tmp/htslib.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/htslib-* /usr/local/src/htslib \
      && cd /usr/local/src/htslib \
      && autoheader \
      && autoconf \
      && ./configure \
      && make \
      && make install

RUN set -e \
      && print-github-tags --release --latest --tar samtools/samtools \
        | xargs -i curl -SL {} -o /tmp/samtools.tar.gz \
      && tar xvf /tmp/samtools.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/samtools-* /usr/local/src/samtools \
      && cd /usr/local/src/samtools \
      && autoheader \
      && autoconf \
      && ./configure \
      && make \
      && make install

RUN set -e \
      && print-github-tags --release --latest --tar samtools/bcftools \
        | xargs -i curl -SL {} -o /tmp/bcftools.tar.gz \
      && tar xvf /tmp/bcftools.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/bcftools-* /usr/local/src/bcftools \
      && cd /usr/local/src/bcftools \
      && make \
      && make install

RUN set -e \
      && pip install -U --no-cache-dir pip /tmp/pandna \
      && rm -rf /tmp/pandna

ENTRYPOINT ["jupyter"]
