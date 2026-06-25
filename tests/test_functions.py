import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from Tproject_etl.utilities import utils


def test_sanitize_column_names(spark: SparkSession):
    """
    Function has to:
    - Replace spaces with underscore
    - Lowercase everything
    - Delete parentesis and comas
    """

    input_columns = ["ID Viaje", "Fecha", "Costo (Cash)", "Origen (Manual)"]
    input_data = [(1, "2026-05-10 03:54:51", 1.67, "Sample Dir")]
    
    # Use the SparkSession to build the dataframe
    input_df = spark.createDataFrame(input_data, schema=input_columns)
    output_df = utils.sanitize_column_names(input_df)
    expected_columns = ["id_viaje", "fecha", "costo_cash", "origen_manual"]

    assert output_df.columns == expected_columns


def test_fix_rating_viaje(spark: SparkSession):
    """
    Validate business logic for 'rating_viaje':
    - Values between 1 and 5 are kept and we mark 'tiene_rating_viaje' as True.
    - Cero values are converted to null and 'tiene_rating_viaje' is marked as False.
    - Null values remaing Null.
    """
    schema_input = StructType([
        StructField("id_viaje", IntegerType(), True),
        StructField("rating_viaje", IntegerType(), True)
    ])

    # Test cases
    input_data = [
        (1, 5),
        (2, 1),
        (3, 0),
        (4, None)
    ]
    
    input_df = spark.createDataFrame(input_data, schema=schema_input)

    output_df = utils.fix_rating_viaje(input_df)

    # Order by id_viaje to ensure correct order when using the collect method
    results = output_df.orderBy("id_viaje").collect()

    assert results[0]["rating_viaje"] == 5
    assert results[0]["tiene_rating_viaje"] is True

    assert results[1]["rating_viaje"] == 1
    assert results[1]["tiene_rating_viaje"] is True

    assert results[2]["rating_viaje"] is None
    assert results[2]["tiene_rating_viaje"] is False

    assert results[3]["rating_viaje"] is None
    assert results[3]["tiene_rating_viaje"] is False


def test_map_estado(spark: SparkSession):
    """
    Tests if numerical codes correspond to the strings below
    - 100 -> en_proceso
    - 200 -> completado
    - 300 -> cancelado
    - Any other (400 or null) -> desconocido
    - Veryfies that col estado has been deleted
    """
    schema_input = StructType([
        StructField("id_viaje", IntegerType(), True),
        StructField("estado", IntegerType(), True)
    ])
    
    input_data = [
        (1, 100),  
        (2, 200),
        (3, 300),
        (4, 400),
        (5, None)
    ]
    
    input_df = spark.createDataFrame(input_data, schema=schema_input)
    output_df = utils.map_estado(input_df)
    results = output_df.orderBy("id_viaje").collect()

    assert "estado" not in output_df.columns
    assert "estado_desc" in output_df.columns

    assert results[0]["estado_desc"] == "en_proceso"
    assert results[1]["estado_desc"] == "completado"
    assert results[2]["estado_desc"] == "cancelado"
    assert results[3]["estado_desc"] == "desconocido"
    assert results[4]["estado_desc"] == "desconocido"


# ----------------Test Cases for parse_comentario_cliente------------------

def test_comentario_vacio_retorna_null(spark):
    schema = StructType([StructField("comentario_cliente", StringType(), True)])
    df = spark.createDataFrame([("",)], schema)
    result = utils.parse_comentario_cliente(df).collect()[0]
    assert result["comentario_cliente"] is None

def test_comentario_null_retorna_null(spark):
    schema = StructType([StructField("comentario_cliente", StringType(), True)])
    df = spark.createDataFrame([(None,)], schema)
    result = utils.parse_comentario_cliente(df).collect()[0]
    assert result["comentario_cliente"] is None

def test_un_tag_con_semicolon_inicial(spark):
    df = spark.createDataFrame([("; Excelente servicio",)], ["comentario_cliente"])
    result = utils.parse_comentario_cliente(df).collect()[0]
    assert result["comentario_cliente"] == ["Excelente servicio"]

def test_multiples_tags(spark):
    raw = "; Vehículo impecable; Puntualidad perfecta; Excelente servicio; Conducción excepcional"
    df = spark.createDataFrame([(raw,)], ["comentario_cliente"])
    result = utils.parse_comentario_cliente(df).collect()[0]
    assert result["comentario_cliente"] == [
        "Vehículo impecable",
        "Puntualidad perfecta",
        "Excelente servicio",
        "Conducción excepcional",
    ]

def test_comentario_mas_tag(spark):
    raw = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus at nisi turpis.; Excelente servicio"
    df = spark.createDataFrame([(raw,)], ["comentario_cliente"])
    result = utils.parse_comentario_cliente(df).collect()[0]
    assert result["comentario_cliente"] == [
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus at nisi turpis.",
        "Excelente servicio"]