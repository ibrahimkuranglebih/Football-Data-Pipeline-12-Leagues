FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_football_data.py .
COPY insert_records.py .

CMD ["python", "insert_records.py"]