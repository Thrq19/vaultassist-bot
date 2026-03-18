FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Buka pintu port 8080 buat dummy web server kita
EXPOSE 8080

CMD ["python", "main.py"]