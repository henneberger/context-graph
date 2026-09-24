FROM python:3.13-alpine
WORKDIR /app
COPY services/check-proxy/server.py .
USER 65532:65532
EXPOSE 8443
ENTRYPOINT ["python","-u","server.py"]
