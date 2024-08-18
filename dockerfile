FROM apache/airflow:2.9.2

USER root

ARG firefox_ver=129.0.1
ARG geckodriver_ver=0.35.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    bzip2 \
    libgl1 \
    libpci3 \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libasound2 \
    && update-ca-certificates \
    \
    # Download and install Firefox
    && curl -fL -o /tmp/firefox.tar.bz2 https://ftp.mozilla.org/pub/firefox/releases/${firefox_ver}/linux-x86_64/en-GB/firefox-${firefox_ver}.tar.bz2 \
    && tar -xjf /tmp/firefox.tar.bz2 -C /tmp/ \
    && mv /tmp/firefox /opt/firefox \
    \
    # Download and install geckodriver
    && curl -fL -o /tmp/geckodriver.tar.gz https://github.com/mozilla/geckodriver/releases/download/v${geckodriver_ver}/geckodriver-v${geckodriver_ver}-linux64.tar.gz \
    && tar -xzf /tmp/geckodriver.tar.gz -C /tmp/ \
    && chmod +x /tmp/geckodriver \
    && mv /tmp/geckodriver /usr/local/bin/ \
    \
    # Cleanup unnecessary stuff
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/* /tmp/*

ENV MOZ_HEADLESS=1

USER airflow
