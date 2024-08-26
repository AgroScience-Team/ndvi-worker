import os
import tempfile

import rasterio.windows
from minio import Minio

from ioc.anotations.beans.component import Component
from ioc.kafka.producers.producer import Producer
from src.domain.workers.abstract_worker import Worker
from src.domain.workers.result import Result


@Component()
class NdviTiffWorker(Worker):

    def __init__(self, producer: Producer) -> None:
        self.minio_client: Minio = Minio(os.getenv("MINIO_URL"),
                                         access_key=os.getenv("MINIO_ROOT_USER"),
                                         secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
                                         secure=False)
        self._chunk_size = 1024
        self._bucket_name = os.getenv("MINIO_BUCKET")
        self._result_topic = os.getenv("kafka.topics.workers-results.name")
        self._producer: Producer = producer

    def process(self, message):
        try:
            with rasterio.Env(AWS_HTTPS='NO', GDAL_DISABLE_READDIR_ON_OPEN='YES', AWS_VIRTUAL_HOSTING=False,
                              AWS_S3_ENDPOINT='localhost:9000'):
                path_red_tiff = f"{message}-red"
                path_nir_tiff = f"{message}-nir"
                with rasterio.open(path_red_tiff) as red_tif, rasterio.open(path_nir_tiff) as nir_tif:
                    profile = red_tif.profile
                    height, width = red_tif.shape

                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
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

                                    red_chunk = red_tif.read(window=window)
                                    nir_chunk = nir_tif.read(window=window)

                                    ndvi_chunk = (nir_chunk - red_chunk) / (nir_chunk + red_chunk)
                                    dst_ndvi.write(ndvi_chunk, window=window)

                        object_name = f"{message}-tiff"
                        self.minio_client.fput_object(self._bucket_name, object_name, output_path,
                                                      content_type='image/tiff')

                        res: Result = Result(photoId=message, result="success", extension="tiff")
                        self._producer.produce(self._result_topic, "ndvi", res.json())
        except Exception as e:
            res: Result = Result(photoId=message, result="error", extension="tiff")
            self._producer.produce(self._result_topic, "ndvi", res.json())
            raise e

    def get_my_key(self) -> str:
        return "tiff"
