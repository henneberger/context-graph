FROM python:3.13-slim
WORKDIR /app
COPY services/search-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/search-api/server.py services/search-api/investigation.py ./
USER 10006
EXPOSE 8443 9404
ENTRYPOINT ["python", "-u", "server.py"]
