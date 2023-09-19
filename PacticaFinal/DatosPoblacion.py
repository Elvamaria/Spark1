# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("SesionDataframes").getOrCreate()

# COMMAND ----------

DatoPoblacion_df = spark.read.csv("dbfs:/FileStore/DatoPoblacion.csv")

# COMMAND ----------

DatoPoblacion_df.show()

# COMMAND ----------

DatoPoblacion_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operaciones con Dataframes

# COMMAND ----------

DatoPoblacion_df.select("hombres2012").show()

# COMMAND ----------

DatoPoblacion_df.select("0 a 4").show()

# COMMAND ----------

DatoPoblacion_df.select("*").show()

# COMMAND ----------

DatoPoblacion_df.select("Etiquetas_fila", "mujeres 2013", "hombres2013").show()

# COMMAND ----------

DatoPoblacion_df.select(employee_df['hombres2013']+10).show()

# COMMAND ----------

employee_df.filter(employee_df['sal'] > 2000).show()

# COMMAND ----------

employee_df.groupBy(employee_df['designation']).count().show()

# COMMAND ----------

DatoPoblacion_df.createOrReplaceTempView("Etiquetas_fila")

# COMMAND ----------

sqlDF = spark.sql("select * from Etiquetas_fila")
sqlDF.show()

# COMMAND ----------

DatoPoblacion_df.createGlobalTempView("etiqueta_fila")

# COMMAND ----------

spark.sql("select * from global_temp.etiqueta_fila").show()

# COMMAND ----------

spark.sql("select count(*) from DatoPoblacion where ename = 'etiqueta_fila'").show()

# COMMAND ----------

DatoPoblacion_df.createOrReplaceTempView("hombres2012")

# COMMAND ----------

spark.sql("select * from Etiquetas_fila a  \
           left join hombres2012 b  on a.deptno = b.deptno").show()

# COMMAND ----------

# se puee realizar tambien fucniones analiticas

spark.sql("select deptno, designation, round(avg(sal) over (partition by deptno),2) mediadeptno from hombres2012").show()

