FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka
COPY produce.py /app/produce.py
CMD ["python", "/app/produce.py"]