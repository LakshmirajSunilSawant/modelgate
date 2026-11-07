FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py loadgen.py oip.py grpc_server.py ./
ENV MODEL_VARIANT=good
EXPOSE 8080
USER 10001
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
