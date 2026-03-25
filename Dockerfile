FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY server.py .

# Railway sets PORT env var
ENV PORT=8000
ENV HOST=0.0.0.0

EXPOSE 8000

CMD ["python", "server.py"]
