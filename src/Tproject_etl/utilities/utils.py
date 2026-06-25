from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def sanitize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalize col names by replacing spaces with underscore "_" and leaving all in lowercase.
    Example: 'Costo (Cash)' -> 'costo_cash'
    """
    new_columns = [
        col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(",", "")
        for col in df.columns
    ]
    return df.toDF(*new_columns)

def fix_rating_viaje(df: DataFrame) -> DataFrame:
    """
    Cleans the rating_viaje column according to data quality rules:
    - Rating of 0 is semantically equivalent to no rating (null), cast to null
    - Empty strings ingested as null remain null
    - Only valid values (1-5) are preserved as integers
    
    Adds:
    - rating_viaje: IntegerType, null when no rating was given
    - tiene_rating_viaje: BooleanType, True when a valid rating exists
    """
    return (
        df
        .withColumn(
            "rating_viaje",
            F.when(F.col("rating_viaje") == 0, F.lit(None).cast("integer"))
             .otherwise(F.col("rating_viaje"))
        )
        .withColumn(
            "tiene_rating_viaje",
            F.col("rating_viaje").isNotNull()
        )
    )

def map_estado(df: DataFrame) -> DataFrame:
    """
    Maps the integer Estado column to a human-readable string.
    
    Mapping (data contract):
        100 -> 'en_proceso'
        200 -> 'completado'
        300 -> 'cancelado'
        any other value -> 'desconocido'
    
    Adds:
    - estado_desc: StringType, replaces the original estado column
    
    Raises a DLT expectation violation if estado_desc ends up null or 'desconocido'
    (wire the expectation in the Silver table definition, not here).
    """
    
    ESTADO_MAP = {
        100: "en_proceso",
        200: "completado",
        300: "cancelado",
    }

    # Build a chained when() from the mapping dict to avoid hardcoding
    mapping_expr = F.lit(None).cast("string")
    for code, label in ESTADO_MAP.items():
        mapping_expr = F.when(F.col("estado") == code, label).otherwise(mapping_expr)

    return (
        df
        .withColumn("estado_desc", mapping_expr)
        .withColumn(
            "estado_desc",
            F.coalesce(F.col("estado_desc"), F.lit("desconocido"))
        )
        .drop("estado")
    )

def parse_comentario_cliente(df: DataFrame) -> DataFrame:
    """
    Parses comentario_cliente into an array of tags.

    Transforms:
      - comentario_cliente: StringType  →  ArrayType(StringType)
        null / empty string             →  null
        any non-empty string            →  array of trimmed non-empty strings

    The original column is replaced in-place (same name, new type).
    """
    return (
        df
        .withColumn(
            "comentario_cliente",
            F.when(
                F.col("comentario_cliente").isNull()
                | (F.trim(F.col("comentario_cliente")) == ""),
                F.lit(None).cast("array<string>")
            )
            .otherwise(
                # 1. trim whitespace
                # 2. strip the leading ";" artifact if present
                # 3. split on ";" into array
                # 4. transform each element: trim inner whitespace
                # 5. filter out empty strings that may remain after split
                F.filter(
                    F.transform(
                        F.split(
                            F.regexp_replace(
                                F.trim(F.col("comentario_cliente")),
                                r"^;",   # remove leading semicolon only
                                ""
                            ),
                            ";"
                        ),
                        lambda tag: F.trim(tag)
                    ),
                    lambda tag: tag != ""
                )
            )
        )
    )
