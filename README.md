# Tarea 3 - Análisis de sensores IoT con Apache Spark y Kafka

## Descripción

Este repositorio contiene la implementación de la Tarea 3 de Big Data de la UNAD, orientada al procesamiento de datos mediante Apache Spark y al procesamiento de información en tiempo real utilizando Apache Kafka y Spark Structured Streaming.

La solución contempla dos escenarios:

1. Procesamiento batch mediante Apache Spark y PySpark sobre un conjunto de datos almacenado en HDFS.
2. Procesamiento de datos en tiempo real mediante Apache Kafka y Spark Structured Streaming, utilizando información simulada de sensores IoT.

## Tecnologías utilizadas

| Tecnología | Versión / configuración |
|---|---|
| Apache Hadoop | 3.3.6 |
| HDFS | Sistema de archivos distribuido utilizado para almacenar el conjunto de datos |
| Apache Spark | 3.5.8 |
| PySpark | API de Python para Apache Spark |
| Apache Kafka | 3.9.2 |
| Java | OpenJDK 11 |
| Python | Python 3 |
| Kafka Topic | `sensor_data` |

## 1. Procesamiento batch con Spark

El procesamiento batch se encuentra implementado en:

`tarea3.py`

El programa establece una sesión de Spark y realiza la lectura de un archivo CSV almacenado en HDFS.

La fuente de datos utilizada es:

```text
hdfs://localhost:9000/Tarea3/rows.csv
```

El archivo contiene información utilizada para realizar diferentes operaciones de análisis mediante DataFrames de Spark.

### Operaciones realizadas

El procesamiento desarrollado incluye:

- Lectura del archivo CSV desde HDFS.
- Inferencia del esquema del conjunto de datos.
- Visualización del esquema mediante `printSchema()`.
- Visualización de los primeros registros.
- Generación de estadísticas descriptivas mediante `summary()`.
- Filtrado de registros cuyo campo `VALOR` es superior a 5000.
- Selección de los campos `VALOR`, `VIGENCIADESDE` y `VIGENCIAHASTA`.
- Ordenamiento de los registros por `VALOR` de forma descendente.

### Código principal

El procesamiento se encuentra disponible en:

```text
tarea3.py
```

### Ejecución

Con Hadoop/HDFS y Spark disponibles en el entorno, el programa puede ejecutarse mediante:

```bash
python3 /home/vboxuser/tarea3.py
```

o mediante:

```bash
spark-submit /home/vboxuser/tarea3.py
```

El archivo de entrada debe encontrarse previamente disponible en HDFS:

```text
/Tarea3/rows.csv
```

## 2. Procesamiento en tiempo real con Kafka y Spark Streaming

El procesamiento en tiempo real se encuentra implementado en:

```text
spark_streaming_consumer.py
```

La solución utiliza Apache Kafka como sistema de mensajería y Spark Structured Streaming como motor de procesamiento.

El consumidor se conecta al broker de Kafka mediante:

```text
localhost:9092
```

y consume información del topic:

```text
sensor_data
```

### Estructura de los datos

Cada mensaje recibido desde Kafka contiene información de un sensor con la siguiente estructura:

```json
{
  "sensor_id": 1,
  "temperature": 25.50,
  "humidity": 60.20,
  "timestamp": "2026-10-27T16:33:06.528409"
}
```

Los campos procesados son:

- `sensor_id`: identificador del sensor.
- `temperature`: temperatura registrada.
- `humidity`: humedad registrada.
- `timestamp`: fecha y hora de la medición.

### Procesamiento realizado

Spark Structured Streaming realiza las siguientes operaciones:

1. Conexión con el broker de Kafka.
2. Suscripción al topic `sensor_data`.
3. Lectura de los mensajes en tiempo real.
4. Conversión del contenido JSON.
5. Aplicación de un esquema estructurado.
6. Agrupación de los datos por sensor.
7. Agrupación mediante ventanas temporales de un minuto.
8. Cálculo del promedio de temperatura.
9. Cálculo del promedio de humedad.
10. Visualización de los resultados en consola.

La ventana utilizada es:

```text
1 minute
```

Los resultados permiten observar el comportamiento promedio de temperatura y humedad para cada sensor durante cada ventana temporal.

### Ejecución

El consumidor se ejecuta mediante Spark Submit con el conector de Kafka para Spark.

Ejemplo:

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
spark_streaming_consumer.py
```

El broker Kafka debe encontrarse disponible previamente en:

```text
localhost:9092
```

y debe existir el topic:

```text
sensor_data
```

## 3. Flujo general de la solución

La arquitectura implementada puede resumirse de la siguiente manera:

```text
                    PROCESAMIENTO BATCH

        rows.csv
            │
            ▼
           HDFS
            │
            ▼
      Apache Spark
         PySpark
            │
            ├── Estadísticas descriptivas
            ├── Filtro VALOR > 5000
            └── Ordenamiento por VALOR
```

Para el procesamiento en tiempo real:

```text
                 PROCESAMIENTO STREAMING

        Datos simulados de sensores
                    │
                    ▼
              Apache Kafka
                    │
              topic sensor_data
                    │
                    ▼
        Spark Structured Streaming
                    │
                    ▼
          Ventana temporal 1 minuto
                    │
                    ▼
       Promedio por sensor
       ├── Temperatura
       └── Humedad
                    │
                    ▼
             Consola Spark
```

## 4. Resultados obtenidos

En el procesamiento batch se trabajó con el conjunto de datos almacenado en HDFS y se realizaron las operaciones de análisis solicitadas, incluyendo estadísticas descriptivas, filtrado y ordenamiento.

En el procesamiento streaming se verificó la recepción continua de mensajes desde Kafka y el procesamiento de las mediciones mediante Spark Structured Streaming.

Los resultados mostraron ventanas temporales asociadas a cada sensor y los promedios calculados para temperatura y humedad.

La ejecución también permitió verificar la actividad de Spark mediante su interfaz web, donde se pueden consultar los Jobs, Stages, Environment y Executors generados durante el procesamiento.

La interfaz de Spark estuvo disponible durante la ejecución mediante:

```text
http://127.0.0.1:4040
```

## 5. Estructura del repositorio

```text
tarea3-bigdata-spark-kafka/
│
├── tarea3.py
├── spark_streaming_consumer.py
└── README.md
```

### `tarea3.py`

Contiene el procesamiento batch realizado con PySpark sobre los datos almacenados en HDFS.

### `spark_streaming_consumer.py`

Contiene el consumidor desarrollado con Spark Structured Streaming para procesar los mensajes publicados en Kafka.

### `README.md`

Contiene la descripción de la solución y las instrucciones generales para ejecutar los componentes desarrollados.

## 6. Evidencias de ejecución

La implementación fue validada mediante la ejecución de los componentes en la máquina virtual utilizada para la práctica.

Entre las evidencias obtenidas se encuentran:

- Ejecución del procesamiento batch.
- Lectura del conjunto de datos desde HDFS.
- Resultados de estadísticas descriptivas.
- Filtrado y ordenamiento de datos.
- Publicación de datos de sensores en Kafka.
- Consumo de mensajes mediante Spark Structured Streaming.
- Resultados agrupados por sensor y ventana temporal.
- Visualización de Jobs de Spark.
- Visualización de Stages.
- Visualización del Environment.
- Visualización de Executors.

Las evidencias gráficas forman parte del documento de entrega de la Tarea 3.

## 7. Video de explicación

Como parte de la presentación online se debe incluir un video explicativo en el que se presenta:

- Introducción al problema.
- Descripción del conjunto de datos seleccionado.
- Explicación del código desarrollado para el procesamiento batch con Spark.
- Ejecución y análisis del procesamiento en tiempo real con Kafka y Spark Streaming.

### Enlace al video

```text
https://drive.google.com/file/d/1lbgrsHkQK5kt_bMo821GKzwXSeNoRtkO/view?usp=drive_link
```

## 8. Repositorio

Código fuente correspondiente a la Tarea 3:

```text
https://github.com/dwilfridolopez-spec/tarea3-bigdata-spark-kafka
```

## 9. Autor

Dwilfrido López

Universidad Nacional Abierta y a Distancia - UNAD
