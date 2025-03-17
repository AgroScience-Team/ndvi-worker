import json
import os
import subprocess
import sys
import time

import pytest
from kafka import KafkaProducer, KafkaConsumer
from minio import Minio
from testcontainers.core.container import DockerContainer
from testcontainers.kafka import KafkaContainer
from testcontainers.minio import MinioContainer

# Константы
BUCKET_NAME = "agro-photos"
INPUT_TOPIC = "agro.workers.ndvi"
OUTPUT_TOPIC = "agro.workers.results"
TEST_NIR = "00000000-0000-0000-0000-000000000001-nir.tiff"
TEST_RED = "00000000-0000-0000-0000-000000000001-red.tiff"

EXPECTED_MESSAGE_PREVIEW = {
    "photoId": "00000000-0000-0000-0000-000000000001",
    "jobId": "00000000-0000-0000-0000-000000000001",
    "workerName": "ndvi-worker",
    "path": "result/00000000-0000-0000-0000-000000000001-ndvi-preview.png",
    "success": True,
    "type": "ndvi-preview"
}

EXPECTED_MESSAGE_NDVI = {
    "photoId": "00000000-0000-0000-0000-000000000001",
    "jobId": "00000000-0000-0000-0000-000000000001",
    "workerName": "ndvi-worker",
    "path": "result/00000000-0000-0000-0000-000000000001-ndvi.tiff",
    "success": True,
    "type": "ndvi"
}

TEST_INPUT_MESSAGE = {
    "jobId": "00000000-0000-0000-0000-000000000001",
    "photoId": "00000000-0000-0000-0000-000000000001",
    "extension": "tiff"
}

def get_url(container: DockerContainer, port: int) -> str:
    return f"{container.get_container_host_ip()}:{container.get_exposed_port(port)}"


@pytest.fixture(scope="module")
def minio_container():
    """Запускает MinIO контейнер и настраивает S3 клиент."""
    with MinioContainer() as minio:
        minio_client = Minio(
            get_url(minio, 9000),
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        minio_client.make_bucket(BUCKET_NAME)
        yield minio, minio_client


@pytest.fixture(scope="module")
def kafka_container():
    """Запускает Kafka контейнер."""
    with KafkaContainer().with_kraft() as kafka:
        producer = KafkaProducer(
            bootstrap_servers=kafka.get_bootstrap_server(),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        consumer = KafkaConsumer(
            OUTPUT_TOPIC,
            bootstrap_servers=kafka.get_bootstrap_server(),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        yield kafka, producer, consumer


def configure_env(minio, kafka):
    """Конфигурирует переменные окружения для приложения."""
    os.environ["kafka.bootstrap-servers"] = kafka.get_bootstrap_server()
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
    os.environ["MINIO_BUCKET"] = BUCKET_NAME
    os.environ["MINIO_URL"] = get_url(minio, 9000)
    os.environ["MINIO_ROOT_USER"] = "minioadmin"
    os.environ["MINIO_ROOT_PASSWORD"] = "minioadmin"
    os.environ["MINIO_CONSOLE_PORT"] = "9090"
    os.environ["MINIO_PORT"] = "9000"
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"


@pytest.fixture(scope="module")
def run_app(minio_container, kafka_container):
    """Запускает приложение с нужными переменными окружения."""

    minio, _ = minio_container
    kafka, _, _ = kafka_container
    configure_env(minio, kafka)

    process = subprocess.Popen(
        ["python3", _absolute_path("../src/main.py")],
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=os.environ
    )

    yield process

    process.terminate()


def test_service_workflow(run_app, minio_container, kafka_container):
    """Основной тест обработки файлов через S3 и Kafka."""
    minio, minio_client = minio_container

    file_path = "converted/"

    minio_client.fput_object(
        BUCKET_NAME, file_path + TEST_NIR, _absolute_path(TEST_NIR)
    )
    minio_client.fput_object(
        BUCKET_NAME, file_path + TEST_RED, _absolute_path(TEST_RED)
    )

    _, producer, consumer = kafka_container

    producer.send(INPUT_TOPIC, TEST_INPUT_MESSAGE)
    producer.flush()
    received_messages = []
    timeout = time.time() + 20

    while time.time() < timeout and len(received_messages) < 2:
        messages = consumer.poll(timeout_ms=1000)
        for _, records in messages.items():
            for record in records:
                received_messages.append(record.value)
                if len(received_messages) == 2:
                    break

    assert len(received_messages) == 2, f"Ожидалось 2 сообщения, но получено {len(received_messages)}"

    assert EXPECTED_MESSAGE_PREVIEW in received_messages, f"Сообщение превью не найдено. Полученные: {received_messages}"
    assert EXPECTED_MESSAGE_NDVI in received_messages, f"Сообщение NDVI не найдено. Полученные: {received_messages}"

    # Проверка, что в MinIO появился обработанный файл
    objects = minio_client.list_objects(bucket_name=BUCKET_NAME, recursive=True)
    files = [obj.object_name for obj in objects]

    expected_files = [
        f"result/00000000-0000-0000-0000-000000000001-ndvi-preview.png",
        f"result/00000000-0000-0000-0000-000000000001-ndvi.tiff"
    ]

    for expected_file in expected_files:
        assert expected_file in files, f"Файл {expected_file} не найден в MinIO"


EXPECTED_MESSAGE_PREVIEW_FAILURE = {
    'jobId': '00000000-0000-0000-0000-000000000001',
    'path': None,
    'photoId': '00000000-0000-0000-0000-000000000001',
    'success': False,
    'type': 'ndvi-preview',
    'workerName': 'ndvi-worker'
}

EXPECTED_MESSAGE_NDVI_FAILURE = {
    'jobId': '00000000-0000-0000-0000-000000000001',
    'path': None,
    'photoId': '00000000-0000-0000-0000-000000000001',
    'success': False,
    'type': 'ndvi',
    'workerName': 'ndvi-worker'
}

TEST_INPUT_MESSAGE_UNKNOWN = {
    "jobId": "00000000-0000-0000-0000-000000000001",
    "photoId": "00000000-0000-0000-0000-000000000001",
    "extension": "unknown"
}

def test_service_workflow_failure(run_app, minio_container, kafka_container):
    """Основной тест обработки файлов через S3 и Kafka."""
    minio, minio_client = minio_container

    # Загрузка файла в MinIO
    file_path = "converted/"

    minio_client.fput_object(
        BUCKET_NAME, file_path + TEST_NIR, _absolute_path(TEST_NIR)
    )
    minio_client.fput_object(
        BUCKET_NAME, file_path + TEST_RED, _absolute_path(TEST_RED)
    )

    _, producer, consumer = kafka_container

    # Отправка сообщения в Kafka
    producer.send(INPUT_TOPIC, TEST_INPUT_MESSAGE_UNKNOWN)
    producer.flush()
    received_messages = []
    timeout = time.time() + 20  # Ожидаем максимум 40 секунд

    while time.time() < timeout and len(received_messages) < 2:
        messages = consumer.poll(timeout_ms=1000)
        for _, records in messages.items():
            for record in records:
                received_messages.append(record.value)
                if len(received_messages) == 2:
                    break

    assert len(received_messages) == 2, f"Ожидалось 2 сообщения, но получено {len(received_messages)}"

    assert EXPECTED_MESSAGE_PREVIEW_FAILURE in received_messages, f"Сообщение превью не найдено. Полученные: {received_messages}"
    assert EXPECTED_MESSAGE_NDVI_FAILURE in received_messages, f"Сообщение NDVI не найдено. Полученные: {received_messages}"


def _absolute_path(filename):
    return os.path.join(os.path.dirname(__file__), filename)