FROM quay.io/ebi-ait/ingest-base-images:python_3.7-alpine
LABEL maintainer="hca-ingest-dev@ebi.ac.uk"

RUN apk update && \
    apk add build-base && \
    apk add openssl-dev && \
    apk add libffi-dev && \
    apk add git

RUN mkdir /app
WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY config.py data_archiver.py listener.py ./

ENV RABBIT_HOST=localhost
ENV RABBIT_PORT=5672
ENV RABBIT_URL=amqp://localhost:5672
ENV INGEST_API=http://localhost:8080
ENV AWS_ACCESS_KEY_ID=
ENV AWS_ACCESS_KEY_SECRET=

ENTRYPOINT ["python"]
CMD ["data_archiver.py"]
