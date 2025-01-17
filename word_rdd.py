from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDD Word Count Example") \
    .getOrCreate()

# load text file into RDD
text_file = spark.sparkContext.textFile("some_words.txt")

# split lines into words, map & reduce by key to count 
counts = (
    text_file.flatMap(lambda line: line.split(" "))  
    .map(lambda word: (word, 1))                    
    .reduceByKey(lambda a, b: a + b)               
)

result = counts.collect()

for word, count in result:
    print(f"{word}: {count}")

spark.stop()