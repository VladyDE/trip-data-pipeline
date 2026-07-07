import pytest
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

# Define the schema of silver so we can create sample silver data
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

GOLD_SQL_PATH = "gold/gold_trips_daily.sql"


def make_row(
    id_viaje,
    fecha,
    placa="AAA3575",
    costo_cash=1.67,
    rating_viaje=5,
    rating_conductor_al_cliente=5,
    comentario_cliente=None,
    comentario_conductor=None,
    nombre_cliente="Jhon Pork",
    direccion_cliente="Feria Libre",
    origen_manual="Feria Libre, General Escandon, Manuel Estrella",
    ingestion_timestamp=None,
    is_quarantined=False,
    tiene_rating_viaje=True,
    estado_desc="completado",
    hora=3,
    dia_semana="Sunday",
    fecha_solo=None,
    semana_anio=19,
    mes=5,
):
    """Helper to build a silver row with sensible defaults, overriding only what a test cares about."""
    return (
        id_viaje,
        fecha,
        placa,
        costo_cash,
        rating_viaje,
        rating_conductor_al_cliente,
        comentario_cliente,
        comentario_conductor,
        nombre_cliente,
        direccion_cliente,
        origen_manual,
        ingestion_timestamp or datetime.now(),
        is_quarantined,
        tiene_rating_viaje,
        estado_desc,
        hora,
        dia_semana,
        fecha_solo or date(2026, 5, 10),
        semana_anio,
        mes,
    )


def run_gold_query(spark, data):
    spark.createDataFrame(data, schema_silver).createOrReplaceTempView("driver_satisfaccion_silver")
    sql = load_gold_sql(GOLD_SQL_PATH)
    return spark.sql(sql)


def test_gold_metricas_diarias_row_count_one_row_per_day(spark):
    # 2 viajes el mismo día, 1 viaje en otro día -> 2 filas (1 por día)
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), fecha_solo=date(2026, 5, 10)),
        make_row(2, datetime(2026, 5, 10, 8, 0), fecha_solo=date(2026, 5, 10)),
        make_row(3, datetime(2026, 3, 14, 3, 0), fecha_solo=date(2026, 3, 14), dia_semana="Saturday", semana_anio=11, mes=3),
    ]

    result = run_gold_query(spark, data)

    assert result.count() == 2


def test_gold_metricas_diarias_total_viajes_y_conductores_activos(spark):
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), placa="AAA3575"),
        make_row(2, datetime(2026, 5, 10, 8, 0), placa="AAA3575"),
        make_row(3, datetime(2026, 5, 10, 9, 0), placa="BBB1234"),
    ]

    result = run_gold_query(spark, data)
    row = result.filter(result.fecha_solo == date(2026, 5, 10)).collect()[0]

    assert row.total_viajes == 3
    assert row.conductores_activos == 2


def test_gold_metricas_diarias_pct_cancelados(spark):
    # 4 viajes: 1 cancelado -> 25% cancelados
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), estado_desc="completado"),
        make_row(2, datetime(2026, 5, 10, 4, 0), estado_desc="completado"),
        make_row(3, datetime(2026, 5, 10, 5, 0), estado_desc="completado"),
        make_row(4, datetime(2026, 5, 10, 6, 0), estado_desc="cancelado"),
    ]

    result = run_gold_query(spark, data)
    row = result.filter(result.fecha_solo == date(2026, 5, 10)).collect()[0]

    assert row.viajes_completados == 3
    assert row.viajes_cancelados == 1
    assert row.pct_cancelados == 25.0


def test_gold_metricas_diarias_ingresos_solo_completados(spark):
    # Solo los viajes completados deben sumar a ingresos_totales
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), costo_cash=10.0, estado_desc="completado"),
        make_row(2, datetime(2026, 5, 10, 4, 0), costo_cash=20.0, estado_desc="completado"),
        make_row(3, datetime(2026, 5, 10, 5, 0), costo_cash=999.0, estado_desc="cancelado"),
    ]

    result = run_gold_query(spark, data)
    row = result.filter(result.fecha_solo == date(2026, 5, 10)).collect()[0]

    assert row.ingresos_totales == 30.0
    assert row.ingreso_promedio_por_viaje == 15.0


def test_gold_metricas_diarias_rating_promedio_ignora_nulos(spark):
    # Un viaje sin rating (None) no debe afectar el promedio ni contar como "con_rating"
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), rating_viaje=5, tiene_rating_viaje=True),
        make_row(2, datetime(2026, 5, 10, 4, 0), rating_viaje=3, tiene_rating_viaje=True),
        make_row(3, datetime(2026, 5, 10, 5, 0), rating_viaje=None, tiene_rating_viaje=False),
    ]

    result = run_gold_query(spark, data)
    row = result.filter(result.fecha_solo == date(2026, 5, 10)).collect()[0]

    assert row.rating_promedio == 4.0
    assert row.viajes_con_rating == 2
    assert float(row.pct_viajes_con_rating) == pytest.approx(2 * 100.0 / 3, abs=0.01)


def test_gold_metricas_diarias_categorias_rating_excelente_y_malo(spark):
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), rating_viaje=5),
        make_row(2, datetime(2026, 5, 10, 4, 0), rating_viaje=5),
        make_row(3, datetime(2026, 5, 10, 5, 0), rating_viaje=1),
        make_row(4, datetime(2026, 5, 10, 6, 0), rating_viaje=3),
    ]

    result = run_gold_query(spark, data)
    row = result.filter(result.fecha_solo == date(2026, 5, 10)).collect()[0]

    assert row.viajes_rating_excelente == 2
    assert row.viajes_rating_malo == 1


def test_gold_metricas_diarias_dimensiones_temporales_propagadas(spark):
    data = [
        make_row(
            1,
            datetime(2026, 5, 10, 3, 0),
            fecha_solo=date(2026, 5, 10),
            dia_semana="Sunday",
            semana_anio=19,
            mes=5,
        ),
    ]

    result = run_gold_query(spark, data)
    row = result.collect()[0]

    assert row.dia_semana == "Sunday"
    assert row.semana_anio == 19
    assert row.mes == 5


def test_gold_metricas_diarias_tipo_dia_fin_de_semana_vs_laboral(spark):
    # 2026-05-10 es domingo (fin de semana); 2026-05-11 es lunes (dia laboral)
    data = [
        make_row(1, datetime(2026, 5, 10, 3, 0), fecha_solo=date(2026, 5, 10)),
        make_row(2, datetime(2026, 5, 11, 3, 0), fecha_solo=date(2026, 5, 11), dia_semana="Monday"),
    ]

    result = run_gold_query(spark, data)
    rows = {r.fecha_solo: r.tipo_dia for r in result.collect()}

    assert rows[date(2026, 5, 10)] == "fin_de_semana"
    assert rows[date(2026, 5, 11)] == "dia_laboral"