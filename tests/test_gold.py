import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import re
from datetime import date, datetime

def load_gold_sql(filepath: str) -> str:
    """
    Reads the .sql file and extracts only the SELECT block,
    stripping USE CATALOG, USE SCHEMA and CREATE OR REPLACE MATERIALIZED VIEW
    so the query runs against temp views in local tests.
    """
    with open(filepath, encoding="utf-8") as f:
        content = f.read()
    
    match = re.search(r"(SELECT\s.+)", content, re.DOTALL | re.IGNORECASE)
    if not match:
        raise ValueError(f"No SELECT statement found in {filepath}")
    
    return match.group(1)

GOLD_OBT_PATH_SQL = "gold/gold_trips_obt.sql"

schema_silver = StructType([
    StructField("id_viaje", IntegerType(), True),
    StructField("fecha", TimestampType(), True),
    StructField("placa", StringType(), True),
    StructField("costo_cash", DoubleType(), True),
    StructField("rating_viaje", IntegerType(), True),
    StructField("rating_conductor_al_cliente", IntegerType(), True),
    StructField("comentario_cliente", ArrayType(StringType()), True),
    StructField("comentario_conductor", StringType(), True),
    StructField("nombre_cliente", StringType(), True),
    StructField("direccion_cliente", StringType(), True),
    StructField("origen_manual", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("is_quarantined", BooleanType(), True),
    StructField("tiene_rating_viaje", BooleanType(), True),
    StructField("estado_desc", StringType(), True),
    StructField("hora", IntegerType(), True),
    StructField("dia_semana", StringType(), True),
    StructField("fecha_solo", DateType(), True),
    StructField("semana_anio", IntegerType(), True),
    StructField("mes", IntegerType(), True)
])

def test_gold_trips_obt_row_count(spark):

    data = [
        (1, datetime(2026, 5, 10, 3, 0), "AAA3575", 1.67, 5, 5, None, None, "Jhon Pork", "Feria Libre", "Feria Libre, General Escandon, Manuel Estrella", datetime.now(), False, True, "completado", 3, "Sunday", date(2026, 5, 10), 19, 5),
        (2, datetime(2026, 3, 14, 3, 0), "AAA3575", 1.67, 5, 5, None, None, "Dorian Lima", "Iglesia El Verbo", "Iglesia El Verbo, Vicente Cuesta, Hortencia Mata", datetime.now(), False, True, "completado", 3, "Saturday", date(2026, 3, 14), 11, 3)
    ]

    # Create the dataframe
    spark.createDataFrame(data, schema_silver).createOrReplaceTempView("driver_satisfaccion_silver")

    # Load the query (string)
    sql = load_gold_sql(GOLD_OBT_PATH_SQL)
    result = spark.sql(sql)

    assert result.count() == 2


def test_gold_trips_obt_effective_income(spark):

    data = [
        # Should show 10 of efective income sinche the trip is completed
        (1, datetime(2026, 5, 10, 14, 0), "AAA3575", 10.0, 5, 5, None, None, "Juan", "Dir", "Orig", datetime.now(), False, True, "completado", 14, "Sunday", date(2026, 5, 10), 19, 5),
        
        # Should show 0 of efective income since the trip was cancelled
        (2, datetime(2026, 5, 10, 15, 0), "BBB1234", 15.0, None, None, None, None, "Pedro", "Dir", "Orig", datetime.now(), False, False, "cancelado", 15, "Sunday", date(2026, 5, 10), 19, 5)
    ]

    spark.createDataFrame(data, schema_silver).createOrReplaceTempView("driver_satisfaccion_silver")

    sql = load_gold_sql(GOLD_OBT_PATH_SQL)
    result = spark.sql(sql).collect()

    completado = next(r for r in result if r["estado"] == "completado")
    cancelado  = next(r for r in result if r["estado"] == "cancelado")

    assert completado["ingreso_completo"] == 10.0
    assert cancelado["ingreso_completo"]  == 0.0


def test_gold_trips_obt_rating_category(spark):

    data = [
        (1, datetime(2026, 5, 10, 10, 0), "AAA3575", 5.0, 5, 5, None, None, "User 1", "Dir", "Orig", datetime.now(), False, True, "completado", 10, "Sunday", date(2026, 5, 10), 19, 5),
        (2, datetime(2026, 5, 10, 11, 0), "AAA3575", 5.0, 4, 4, None, None, "User 2", "Dir", "Orig", datetime.now(), False, True, "completado", 11, "Sunday", date(2026, 5, 10), 19, 5),
        (3, datetime(2026, 5, 10, 12, 0), "AAA3575", 5.0, 1, 1, None, None, "User 3", "Dir", "Orig", datetime.now(), False, True, "completado", 12, "Sunday", date(2026, 5, 10), 19, 5),
        (4, datetime(2026, 5, 10, 13, 0), "AAA3575", 5.0, None, None, None, None, "User 4", "Dir", "Orig", datetime.now(), False, False, "completado", 13, "Sunday", date(2026, 5, 10), 19, 5)
    ]

    spark.createDataFrame(data, schema_silver).createOrReplaceTempView("driver_satisfaccion_silver")

    sql = load_gold_sql(GOLD_OBT_PATH_SQL)
    result = {r["id_viaje"]: r["categoria_rating"] for r in spark.sql(sql).collect()}

    assert result[1] == "excelente"
    assert result[2] == "bueno"
    assert result[3] == "malo"
    assert result[4] == "sin_rating"