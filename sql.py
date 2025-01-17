import shutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# clean up existing directory
warehouse_dir = "/Users/larsswenson/Desktop/NSC/AD350/ApacheSparkExamples/spark-warehouse/some_people"
if os.path.exists(warehouse_dir):
    shutil.rmtree(warehouse_dir)

spark = SparkSession.builder.appName("SQLExample").getOrCreate()

df = spark.createDataFrame([
    ("Cynthia", 43), 
    ("Bob", 75), 
    ("Fela", 69), 
    ("Francoise", 18),
    ("Emil", 4)
], ["first_name", "age"])

# add column
df1 = df.withColumn(
    "life_stage",
    when(col("age") < 13, "child")
    .when(col("age").between(13, 19), "teenager")
    .otherwise("adult"),
)

df1.createOrReplaceTempView("people")

# average age
spark.sql("SELECT AVG(age) FROM people").show()

# average age by life_stage
spark.sql("SELECT life_stage, AVG(age) FROM people GROUP BY life_stage").show()

# drop & recreate the table
spark.sql("DROP TABLE IF EXISTS some_people")
df1.write.mode("overwrite").saveAsTable("some_people")

# insert new row
spark.sql("INSERT INTO some_people VALUES ('Cecilia', 3, 'child')")

# verify insert
spark.sql("SELECT * FROM some_people").show()

spark.stop()

