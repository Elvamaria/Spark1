# Databricks notebook source
# MAGIC %md
# MAGIC ## Creacion de tabla para regresión

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField

schema = StructType([
    StructField("Etiquetas_fila", StringType(), True),
    StructField("hombres2012", IntegerType(), True),
    StructField("mujeres2012", IntegerType(), True),
    StructField("hombres2013", IntegerType(), True),
    StructField("mujeres2013", IntegerType(), True),
    StructField("hombres2014", IntegerType(), True),
    StructField("mujeres2014", IntegerType(), True),
    StructField("hombres2015", IntegerType(), True),
    StructField("mujeres2015", IntegerType(), True),
    StructField("hombres2016", IntegerType(), True),
    StructField("mujeres2016", IntegerType(), True),
    StructField("hombres2017", IntegerType(), True),
    StructField("mujeres2017", IntegerType(), True),
    StructField("hombres2018", IntegerType(), True),
    StructField("mujeres2018", IntegerType(), True),
    StructField("hombres2019", IntegerType(), True),
    StructField("mujeres2019", IntegerType(), True),
    StructField("hombres2020", IntegerType(), True),
    StructField("mujeres2020", IntegerType(), True),
    StructField("hombres2021", IntegerType(), True),
    StructField("mujeres2021", IntegerType(), True),
    StructField("hombres2022", IntegerType(), True),
    StructField("mujeres2022", IntegerType(), True)
])

DatoPoblacion_df = spark.read.format("csv").schema(schema).option("header", "true").load("/FileStore/DatoPoblacion.csv")

# COMMAND ----------

DatoPoblacion_df.write.saveAsTable("default.datopoblacion")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datospoblacion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creacion de tabla para clasificación

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
 
schema = StructType([
    StructField("Etiquetas_fila", StringType(), True),
    StructField("hombres2012", IntegerType(), True),
    StructField("mujeres2012", IntegerType(), True),
    StructField("hombres2013", IntegerType(), True),
    StructField("mujeres2013", IntegerType(), True),
    StructField("hombres2014", IntegerType(), True),
    StructField("mujeres2014", IntegerType(), True),
    StructField("hombres2015", IntegerType(), True),
    StructField("mujeres2015", IntegerType(), True),
    StructField("hombres2016", IntegerType(), True),
    StructField("mujeres2016", IntegerType(), True),
    StructField("hombres2017", IntegerType(), True),
    StructField("mujeres2017", IntegerType(), True),
    StructField("hombres2018", IntegerType(), True),
    StructField("mujeres2018", IntegerType(), True),
    StructField("hombres2019", IntegerType(), True),
    StructField("mujeres2019", IntegerType(), True),
    StructField("hombres2020", IntegerType(), True),
    StructField("mujeres2020", IntegerType(), True),
    StructField("hombres2021", IntegerType(), True),
    StructField("mujeres2021", IntegerType(), True),
    StructField("hombres2022", IntegerType(), True),
    StructField("mujeres2022", IntegerType(), True)
])
DatoPoblacion_df = spark.read.format("csv").schema(schema).load("/databricks-datasets/adult/adult.data")

# COMMAND ----------

DatoPoblacion_df.write.saveAsTable("default.datoscocha")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datoscocha
