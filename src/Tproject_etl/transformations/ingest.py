from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *
#from utilities import utils
from Tproject_etl.utilities import utils

# Landing zone path
file_path="/Volumes/azu/default/driversatisfaccion_landingdata/"


# Define the initial schema according to data contracts
schema = StructType([
    StructField("ID Viaje", IntegerType(), True),
    StructField("Fecha", TimestampType(), True),
    StructField("Placa", StringType(), True),
    StructField("Costo (Cash)", DoubleType(), True),
    StructField("Estado", IntegerType(), True),
    StructField("Rating Viaje", IntegerType(), True),
    StructField("Rating Conductor al Cliente", IntegerType(), True),
    StructField("Comentario Cliente", StringType(), True),
    StructField("Comentario Conductor", StringType(), True),
    StructField("Nombre Cliente", StringType(), True),
    StructField("Direccion Cliente", StringType(), True),
    StructField("Origen (Manual)", StringType(), True)
])

@dp.table(
    name="driver_satisfaccion_bronze",
    comment="Raw driver satisfaccion data ingested from csv files in volume"
)
def driver():
    df_raw = (spark.readStream
        .format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ",")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(file_path)
    )
    
    # Apply small transformations to add ingestion metadata and column sanitization
    df_with_timestamp = df_raw.withColumn("ingestion_timestamp", F.current_timestamp())
    
    df_sanitized = utils.sanitize_column_names(df_with_timestamp)
    
    return df_sanitized
