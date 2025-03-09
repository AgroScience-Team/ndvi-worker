import os
import tempfile

import matplotlib.pyplot as plt
import numpy as np
import rasterio
from matplotlib import colors
from minio import Minio
from rasterio.enums import Resampling

from ioc.anotations.beans.component import Component
from ioc.anotations.proxy.log.log import Log
from ioc.common_logger import log
from ioc.kafka.producers.producer import Producer
from src.domain.models.worker_input import WorkerInput
from src.domain.workers.abstract_worker import Worker
from src.domain.models.result import Result, result_factory


class MidpointNormalize(colors.Normalize):
    def __init__(self, vmin=None, vmax=None, midpoint=None, clip=False):
        self.midpoint = midpoint
        colors.Normalize.__init__(self, vmin, vmax, clip)

    def __call__(self, value, clip=None):
        x, y = [self.vmin, self.midpoint, self.vmax], [0, 0.5, 1]
        return np.ma.masked_array(np.interp(value, x, y), np.isnan(value))


@Component()
class NdviTiffWorker(Worker):

    def __init__(self, producer: Producer) -> None:
        self.minio_client: Minio = Minio(os.getenv("MINIO_URL"),
                                         access_key=os.getenv("MINIO_ROOT_USER"),
                                         secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
                                         secure=False)
        self._chunk_size = 1024
        self._bucket_name = os.getenv("MINIO_BUCKET")
        self._result_topic = "agro.workers.results"
        self._producer: Producer = producer
        self._key = "tiff"
        self._prefix = "converted"
        self._result = "result"

    @Log()
    def process(self, message: WorkerInput):
        try:
            with rasterio.Env(AWS_HTTPS='NO', GDAL_DISABLE_READDIR_ON_OPEN='YES', AWS_VIRTUAL_HOSTING=False,
                              AWS_S3_ENDPOINT=os.getenv("MINIO_URL")):
                path_red_tiff = f"/vsis3/{self._bucket_name}/{self._prefix}/{message.photoId}-red.{self._key}"
                path_nir_tiff = f"/vsis3/{self._bucket_name}/{self._prefix}/{message.photoId}-nir.{self._key}"

                with rasterio.open(path_red_tiff) as red_tif, rasterio.open(path_nir_tiff) as nir_tif:
                    profile = red_tif.profile.copy()
                    profile.update(count=1, dtype=rasterio.float32)
                    height, width = red_tif.shape

                    log.info(f"Start computing NDVI")
                    # Compute NDVI
                    with tempfile.NamedTemporaryFile(suffix=f"ndvi.{self._key}", delete=True) as temp_file:
                        output_path = temp_file.name
                        with rasterio.open(output_path, 'w', **profile) as dst_ndvi:
                            for offset_y in range(0, height, self._chunk_size):
                                for offset_x in range(0, width, self._chunk_size):
                                    window = rasterio.windows.Window(
                                        offset_x,
                                        offset_y,
                                        min(self._chunk_size, width - offset_x),
                                        min(self._chunk_size, height - offset_y)
                                    )

                                    red_chunk = red_tif.read(window=window).astype(rasterio.float32)
                                    nir_chunk = nir_tif.read(window=window).astype(rasterio.float32)

                                    ndvi_chunk = (nir_chunk - red_chunk) / (nir_chunk + red_chunk)
                                    dst_ndvi.write(ndvi_chunk, window=window)

                        # Save the original NDVI to S3
                        object_name = f"{self._result}/{message.photoId}-ndvi.{self._key}"
                        self.minio_client.fput_object(self._bucket_name, object_name, output_path,
                                                      content_type='image/tiff')

                        red_tif.close()
                        nir_tif.close()

                        # Create a lightweight preview (PNG) and upload to S3
                        with tempfile.NamedTemporaryFile(suffix=f"preview.png",
                                                         delete=True) as preview_temp_file:
                            preview_path = preview_temp_file.name
                            self.create_preview(output_path, preview_path)

                            # Upload the preview to S3
                            preview_object_name = f"{self._result}/{message.photoId}-ndvi-preview.png"
                            self.minio_client.fput_object(self._bucket_name, preview_object_name, preview_path,
                                                          content_type='image/png')

                        # Produce the result message
                        log.info(f"Finish computing NDVI")
                        res: Result = result_factory(message, object_name, True, "ndvi")
                        log.info(f"Send result: {res}")
                        self._producer.produce(self._result_topic, "ndvi", res.json())
                        res_preview: Result = result_factory(message, preview_object_name, True, "ndvi-preview")
                        log.info(f"Send result: {res_preview}")
                        self._producer.produce(self._result_topic, "ndvi-preview", res_preview.json())

        except Exception as e:
            log.warn("Computing NDVI failed")
            res: Result = result_factory(message, None, False, "ndvi-preview")
            self._producer.produce(self._result_topic, "", res.json())
            res: Result = result_factory(message, None, False, "ndvi")
            self._producer.produce(self._result_topic, "", res.json())
            raise e

    def create_preview(self, ndvi_path: str, preview_path: str) -> None:
        log.info("Start creating preview")
        """
        Создает легковесное превью NDVI в формате JPEG, окрашенное в градации красного, желтого и зеленого.
        Снижает потребление памяти за счет уменьшения разрешения изображения.
        """
        # Открываем TIFF с NDVI
        with rasterio.open(ndvi_path) as src:
            # Даунсемплинг: уменьшаем размер в 4 раза (изменяем при необходимости)
            scale_factor = 4  # 1/4 оригинального размера

            new_width = src.width // scale_factor
            new_height = src.height // scale_factor

            # Читаем данные с уменьшением разрешения
            ndvi = src.read(
                1,
                out_shape=(new_height, new_width),
                resampling=Resampling.average  # Используем усреднение пикселей
            )

        # Задаем минимальное и максимальное значение NDVI
        min_val = np.nanmin(ndvi)
        max_val = np.nanmax(ndvi)
        midpoint = 0.1

        # Устанавливаем цветовую карту и нормализацию
        colormap = plt.cm.RdYlGn  # Красно-желто-зеленый градиент
        norm = MidpointNormalize(vmin=min_val, vmax=max_val, midpoint=midpoint)

        # Создаем изображение
        fig, ax = plt.subplots(figsize=(6, 6), dpi=150)
        ax.imshow(ndvi, cmap=colormap, norm=norm)
        ax.axis('off')  # Отключаем оси

        # Сохраняем в JPEG с сжатием
        fig.savefig(preview_path, format='jpeg', dpi=100, bbox_inches='tight', pad_inches=0.1)
        plt.close(fig)  # Освобождаем память

    def get_my_key(self) -> str:
        return self._key
