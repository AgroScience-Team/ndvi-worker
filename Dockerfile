FROM python:3.10-slim

RUN pip install poetry

WORKDIR /ndvi-worker

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root
RUN apt update && apt install -y libexpat1

COPY ioc ./ioc
COPY src ./src

ENV PYTHONPATH "${PYTHONPATH}:/ndvi-worker"

CMD ["poetry", "run", "python", "./src/main.py"]