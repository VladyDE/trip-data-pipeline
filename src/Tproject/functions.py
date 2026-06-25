from pyspark.sql import DataFrame

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