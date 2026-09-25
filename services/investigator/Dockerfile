# Build the pinned upstream runner with scripts/build-ax-tools.sh first.
FROM python:3.13-slim
RUN pip install --no-cache-dir urllib3==2.6.3
COPY services/investigator/investigator.py /app/investigator.py
COPY .build/ax-task-runner /usr/local/bin/ax-task-runner
RUN mkdir -p /workspace /ax
EXPOSE 80
ENTRYPOINT ["/usr/local/bin/ax-task-runner"]
