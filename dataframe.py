from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

spark = SparkSession.builder.appName("Test").getOrCreate()
data = [("Cynthia", 43), ("Bob", 75), ("Fela", 69), ("Francoise", 18), ("Emil", 4)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

# add column
df1 = df.withColumn(
    "life_stage",
    when(col("age") < 13, "child")
    .when(col("age").between(13, 19), "teenager")
    .otherwise("adult"),
)
df1.show()

# filter dataframe
df1.where(col("life_stage").isin(["teenager", "adult"])).show()

# average age & group by aggregation 
df1.select(avg("age")).show()

df1.groupBy("life_stage").avg("age").show()



