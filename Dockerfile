FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app:/app/shared/generated
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN mkdir -p shared/generated && \
    python -m grpc_tools.protoc -Ishared/proto \
      --python_out=shared/generated --grpc_python_out=shared/generated \
      shared/proto/flight.proto
