from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()

df = spark.read.csv("customers.csv", header=True, inferSchema=True)

df = df.dropna().dropDuplicates()

df = df.withColumn(
    "Customer_Type",
    when(df["Annual_Income"] > 50000, "High Value").otherwise("Low Value")
)

df.groupBy("Customer_Type").count().show()

df.write.mode("overwrite").csv("output", header=True)
