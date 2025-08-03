FROM python:3.11-slim

WORKDIR /app

COPY main.py .

RUN pip install fastapi duckdb uvicorn pytz python-dateutil pandas

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]