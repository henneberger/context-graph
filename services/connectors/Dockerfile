FROM python:3.13-slim
WORKDIR /app
COPY services/connectors/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/connectors/*.py ./
USER 10005
EXPOSE 9404
ENTRYPOINT ["python", "-u", "worker.py"]
