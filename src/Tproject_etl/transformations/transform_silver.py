from pyspark import pipelines as dp
from pyspark.sql import functions as F
#from utilities import utils
from Tproject_etl.utilities import utils

def get_rules(tag):
  """
    Loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  #--------------------------Leaving rules table name hardcoded, will change it with params for scalability in the near future--------------------
  rules_df = spark.read.table("azu.default.rules").filter(F.col("tag") == tag).collect()
  return {
      row['name']: row['constraint']
      for row in rules_df
  }


@dp.table(
    name="driver_satisfaccion_silver",
    comment="Cleaned and enriched driver satisfaction data. Includes rating nullability fix, estado mapping, and temp features."
)
@dp.expect_all_or_drop(get_rules('validity'))

def driver_satisfaccion_silver():
    df = spark.readStream.table("driver_satisfaccion_bronze")
    
    # Apply transformations to fix rating, map state of the row and create an array of tags (not just string as tags)
    df = utils.fix_rating_viaje(df)
    df = utils.map_estado(df)
    df = utils.parse_comentario_cliente(df)

    # Time features
    df = (
        df
        .withColumn("hora",           F.hour("fecha"))
        .withColumn("dia_semana",     F.date_format("fecha", "EEEE"))
        .withColumn("fecha_solo",     F.to_date("fecha"))
        .withColumn("semana_anio",    F.weekofyear("fecha"))
        .withColumn("mes",            F.month("fecha"))
    )

    return df


'''
@dp.table(
   name='driver_satisfaccion_quarantine_silver',
   comment='Contains rows that failed the data quality expectations so they can be audit later, this table is append-only behaviour'
)
def driver_satisfaccion_quarantine():
    df_bronze = spark.readStream.table("driver_satisfaccion_bronze")
    quarantine_rules = "NOT({0})".format(" AND ".join(get_rules('validity').values()))
    df_errors = (
        df_bronze
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )
    
    return df_errors
'''