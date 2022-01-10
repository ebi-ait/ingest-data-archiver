FROM quay.io/ebi-ait/ingest-base-images:python_3.7-alpine
LABEL maintainer="hca-ingest-dev@ebi.ac.uk"

RUN pip install --upgrade pip
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

ENV INGEST_API=http://localhost:8080
ENV INGEST_S3_BUCKET=org-hca-data-archive-upload-dev
ENV INGEST_S3_REGION=us-east-1
ENV RABBIT_HOST=localhost
ENV RABBIT_PORT=5672
ENV AWS_ACCESS_KEY_ID=
ENV AWS_ACCESS_KEY_SECRET=
ENV ENA_FTP_HOST=webin.ebi.ac.uk
ENV ENA_FTP_DIR=dev
ENV ENA_WEBIN_USERNAME=Webin-46220
ENV ENA_WEBIN_PASSWORD=
ENV ARCHIVER_DATA_DIR=/data

ENTRYPOINT ["python"]
CMD ["data_archiver.py"]
