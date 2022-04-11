FROM python:3.9-slim-buster
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV RUNNING_IN_CONTAINER Yes
WORKDIR /app
RUN apt update && apt install -y fping git
RUN pip install --no-cache-dir poetry
COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root
COPY . .
RUN poetry install --no-interaction --no-ansi
WORKDIR /app/invest_ohlcv_api_to_kafka
ENTRYPOINT ["python","main.py"]