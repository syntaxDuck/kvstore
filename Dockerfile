FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install uv && uv pip install --system -r pyproject.toml

COPY src/ src/
COPY main.py .

RUN mkdir -p /app/data

EXPOSE 5003 8080

CMD ["python", "main.py"]
