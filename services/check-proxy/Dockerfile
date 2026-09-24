FROM python:3.13-alpine
WORKDIR /app
COPY services/check-proxy/server.py .
COPY services/observability/http_metrics.py .
USER 65532:65532
EXPOSE 8443
ENTRYPOINT ["python","-u","server.py"]
