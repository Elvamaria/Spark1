# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

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

DatoPoblacion_df.display()

# COMMAND ----------

DatoPoblacion_df.count()

# COMMAND ----------

DatoPoblacion_df.columns

# COMMAND ----------

DatoPoblacion_df.dtypes

# COMMAND ----------

type(DatoPoblacion_df)

# COMMAND ----------

train_df, test_df = DatoPoblacion_df.randomSplit([0.99, 0.01], seed=42)

# COMMAND ----------

from databricks import automl

# COMMAND ----------

summary = automl.regress(train_df, target_col="median_house_value", timeout_minutes=5)

# COMMAND ----------

print(summary)

# COMMAND ----------

print(summary.best_trial.model_path)

# COMMAND ----------

import mlflow
 
model_uri = f"runs:/{summary.best_trial.mlflow_run_id}/model"
 
predict = mlflow.pyfunc.spark_udf(spark, model_uri)
pred_df = test_df.withColumn("prediction", predict(*test_df.drop("median_house_value").columns))
display(pred_df)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
 
regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="median_house_value", metricName="r2")
rmse = regression_evaluator.evaluate(pred_df)
print(f"val_r2_score on test dataset: {rmse:.3f}")
