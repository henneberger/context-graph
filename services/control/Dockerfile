FROM python:3.13-slim
WORKDIR /app
COPY services/control/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/control/server.py .
USER 10004
EXPOSE 8443 9404
ENTRYPOINT ["python", "-u", "server.py"]
