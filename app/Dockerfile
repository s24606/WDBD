FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY load_data.py .
COPY postgresql_insert.py .
CMD ["python", "load_data.py"]