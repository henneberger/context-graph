FROM python:3.13-slim
WORKDIR /app
COPY services/mcp/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/mcp/server.py .
USER 10005
EXPOSE 8443 9404
ENTRYPOINT ["python","server.py"]
