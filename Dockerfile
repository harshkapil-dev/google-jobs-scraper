FROM mcr.microsoft.com/playwright/python:v1.62.0-noble
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chown -R pwuser:pwuser /app
USER pwuser
EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --no-access-log --timeout-graceful-shutdown 30"]
