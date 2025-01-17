from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, split

spark = SparkSession.builder.appName("StreamingExample").getOrCreate()

# Kafka config
kafka_bootstrap_servers = "localhost:9092"
subscribe_topic = "student_topic"

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", subscribe_topic)
    .load()
)

schema = StructType([
    StructField("student_name", StringType()),
    StructField("graduation_year", StringType()),
    StructField("major", StringType())
])

# transform data
def with_normalized_names(df):
    parsed_df = (
        df.withColumn("json_data", from_json(col("value").cast("string"), schema))
        .selectExpr("json_data.*")
    )
    split_col = split(parsed_df["student_name"], "XX")
    return parsed_df.withColumn("first_name", split_col.getItem(0)).withColumn("last_name", split_col.getItem(1))

# write to Parquet
query = with_normalized_names(df).writeStream \
    .format("parquet") \
    .option("path", "data/tmp_students") \
    .option("checkpointLocation", "data/tmp_students_checkpoint") \
    .start()

query.awaitTermination()
