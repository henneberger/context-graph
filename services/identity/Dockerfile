FROM python:3.13-slim
WORKDIR /app
COPY services/identity/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/identity/server.py .
COPY services/observability/http_metrics.py .
USER 10003
EXPOSE 8443
ENTRYPOINT ["python","server.py"]
