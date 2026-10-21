from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd

# 1. Inicializar SparkSession
spark = SparkSession.builder \
    .appName("AnalisisSaludBatch") \
    .getOrCreate()

# 2. Cargar el conjunto de datos histórico (asegúrate de tener este archivo)
# Si no tienes uno, puedes crear un CSV de prueba con algunas filas.
try:
    df = spark.read.csv("https://www.datos.gov.co/api/v3/views/4k9h-8qiu/query.csv", header=True, inferSchema=True)
except Exception as e:
    print("Error al cargar el CSV. Asegúrate de que historico_urgencias.csv exista en esta ruta.")
    spark.stop()
    exit()

# 3. Limpieza: Eliminar registros donde no se registró el nivel de Triage
df_clean = df.dropna(subset=["nivel_triage"])

# 4. Análisis Exploratorio (EDA): Contar el volumen de pacientes por Nivel de Triage
tendencia_triage_df = df_clean.groupBy("nivel_triage").count().orderBy("count", ascending=False)

print("--- Distribución Histórica de Pacientes por Triage ---")
tendencia_triage_df.show()

# 5. Visualización: Guardar la gráfica como imagen en el servidor
pandas_df = tendencia_triage_df.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(pandas_df['nivel_triage'], pandas_df['count'], color=['red', 'orange', 'yellow', 'green', 'blue'])
plt.title('Volumen de Pacientes por Nivel de Triage (Histórico)')
plt.xlabel('Nivel de Triage')
plt.ylabel('Cantidad de Pacientes')
plt.xticks(rotation=45)
plt.tight_layout()

# Guardamos la imagen en lugar de usar plt.show()
plt.savefig("grafica_triage_batch.png")
print("Gráfica guardada exitosamente como 'grafica_triage_batch.png'")

spark.stop()
