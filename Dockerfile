FROM python:3.10-slim

RUN pip install poetry

WORKDIR /ndvi-worker

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root

COPY ioc ./ioc
COPY src ./src

ENV PYTHONPATH "${PYTHONPATH}:/ndvi-worker"

RUN poetry install

CMD ["poetry", "run", "python", "./src/main.py"]