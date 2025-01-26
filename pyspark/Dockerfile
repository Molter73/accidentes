FROM docker.io/bitnami/spark:3.5.4

COPY requirements.txt /opt/bitnami/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt && \
    rm -f /opt/bitnami/spark/requirements.txt
