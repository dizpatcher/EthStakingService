FROM python:3.11.6-bookworm as builder
WORKDIR /app

COPY . /app
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

FROM gcr.io/distroless/python3-debian12:nonroot

COPY --from=builder /app /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /app/service /usr/local/bin/service

COPY --from=builder /app/configs configs

ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages

ENTRYPOINT [ "python", "/app/main.py" ]