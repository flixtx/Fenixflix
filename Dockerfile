FROM python:3.10-slim

WORKDIR /app

COPY . .

# Instala dependências básicas e o Node.js 20.x
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Instala dependências do Python
RUN pip install --no-cache-dir -r requirements.txt


EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
