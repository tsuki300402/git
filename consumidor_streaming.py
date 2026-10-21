from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("MonitoreoUrgenciasStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema JSON exacto que envía nuestro productor
esquema_urgencias = StructType([
    StructField("id_sede_hospital", IntegerType()),
    StructField("nivel_triage", StringType()),
    StructField("timestamp", DoubleType())
])

# 1. Conectar a Kafka y leer el topic 'flujo_urgencias'
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flujo_urgencias") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parsear el valor de Kafka (que viene en binario) a JSON y luego a columnas
datos_parseados = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema_urgencias).alias("datos")) \
    .select("datos.*")

# 3. Procesamiento en tiempo real: Contar cuántos pacientes han ingresado por cada sede
# Esto es vital para saber si una sede está superando su capacidad operativa
conteo_por_sede = datos_parseados.groupBy("id_sede_hospital").count()

# 4. Mostrar la tabla de congestión actualizándose en la consola
query = conteo_por_sede.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
