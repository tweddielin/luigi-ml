FROM python:3.7-slim

RUN apt-get update -y && \
        apt-get install -y --no-install-recommends gnupg2 wget && \
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
        echo "deb http://apt.postgresql.org/pub/repos/apt/ buster-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list && \
        apt-get update -y && \
        apt-get install -y --no-install-recommends postgresql-client-12

COPY . /app

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 9091

ENV WAIT_VERSION 2.7.2
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait /wait
RUN chmod +x /wait



CMD ["luigid", "--port=9091"]

