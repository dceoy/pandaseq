FROM dceoy/jupyter:latest

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
      && curl -sS https://github.com/samtools/htslib/releases/latest \
        | sed -e 's/^.*"\(https:\/\/github.com\/[^\/]\+\/[^\/]\+\/\)releases\/tag\(\/[^"]\+\)".*$/\1archive\2\.tar\.gz/' \
        | xargs -i wget -L {} -O /tmp/htslib.tar.gz \
      && tar xvf /tmp/htslib.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/htslib-* /usr/local/src/htslib \
      && cd /usr/local/src/htslib \
      && autoheader \
      && autoconf \
      && ./configure \
      && make \
      && make install

RUN set -e \
      && curl -sS https://github.com/samtools/samtools/releases/latest \
        | sed -e 's/^.*"\(https:\/\/github.com\/[^\/]\+\/[^\/]\+\/\)releases\/tag\(\/[^"]\+\)".*$/\1archive\2\.tar\.gz/' \
        | xargs -i wget -L {} -O /tmp/samtools.tar.gz \
      && tar xvf /tmp/samtools.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/samtools-* /usr/local/src/samtools \
      && cd /usr/local/src/samtools \
      && autoheader \
      && autoconf \
      && ./configure \
      && make \
      && make install

RUN set -e \
      && curl -sS https://github.com/samtools/htslib/releases/latest \
        | sed -e 's/^.*"\(https:\/\/github.com\/[^\/]\+\/[^\/]\+\/\)releases\/tag\(\/[^"]\+\)".*$/\1archive\2\.tar\.gz/' \
        | xargs -i wget -L {} -O /tmp/htslib.tar.gz \
      && tar xvf /tmp/htslib.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/htslib-* /usr/local/src/htslib \
      && curl -sS https://github.com/samtools/bcftools/releases/latest \
        | sed -e 's/^.*"\(https:\/\/github.com\/[^\/]\+\/[^\/]\+\/\)releases\/tag\(\/[^"]\+\)".*$/\1archive\2\.tar\.gz/' \
        | xargs -i wget -L {} -O /tmp/bcftools.tar.gz \
      && tar xvf /tmp/bcftools.tar.gz -C /usr/local/src --remove-files \
      && mv /usr/local/src/bcftools-* /usr/local/src/bcftools \
      && cd /usr/local/src/bcftools \
      && make \
      && make install

RUN set -e \
      && pip install -U --no-cache-dir pip /tmp/pandna \
      && rm -rf /tmp/pandna

ENTRYPOINT ["jupyter"]
