FROM bitnami/spark:3.5

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY spark_streaming_app.py .
COPY utils ./utils

CMD ["python", "spark_streaming_app.py"]
