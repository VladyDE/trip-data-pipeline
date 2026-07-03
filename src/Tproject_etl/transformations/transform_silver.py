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

# Quarantined rules, iteracion was added (this is diff from databricks docs) in order to handle multiple conditions within a single expectation
quarantine_rules = "NOT({0})".format(" AND ".join(f"({rule})" for rule in get_rules('validity').values()))

# Staging temporary table for data quality checks
@dp.table(
  temporary=True,
  partition_cols=["is_quarantined"],
)
@dp.expect_all(get_rules('validity'))
def driver_data_quarantine():
  return (
    spark.readStream.table("driver_satisfaccion_bronze").withColumn("is_quarantined", F.expr(quarantine_rules))
  )

@dp.table(
    name="driver_satisfaccion_silver",
    comment="Cleaned and enriched driver satisfaction data. Includes rating nullability fix, estado mapping, and temp features."
)

def driver_satisfaccion_silver():
    df = spark.readStream.table("driver_data_quarantine").filter("is_quarantined=false")
    
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

@dp.table(
    name="driver_satisfaccion_quarantined_silver",
    comment="Quarantined data from driver table that not met the data quality expectations."
)
def driver_satisfaccion_quarantined_silver():
   df=spark.readStream.table("driver_data_quarantine").filter("is_quarantined=true")
   df_withT=df.withColumn("quarantined_timestamp", F.current_timestamp())

   return df_withT