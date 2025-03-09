# Обработка файлов с расширением tif

1) Сервис [слушает](../src/domain/listener/ndvi_worker_listener.py) сообщения из топика ndvi.
2) Если расширение поддерживается, то задача передаётся локальному [воркеру](../src/domain/workers/ndvi_tiff_worker.py)
3) Обработка выглядит следующим образом:  
   Получаемое сообщение kafka:

```
key: tif
value: uuid-example
```

Воркер пытается найти в s3 файлы формата uuid-example-nir.tif и uuid-example-red.tif. В случае успеха  
считается ndvi и сохраняется по шаблону uuid-example-ndvi.tif.

4) [Результат](../src/domain/models/result.py) работы сохраняется в топик workers.results
