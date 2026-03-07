FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install uv && uv pip install --system -r pyproject.toml

COPY src/ src/
COPY main.py .

EXPOSE 5003

CMD ["python", "main.py"]
