from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

jars_path = "/home/paola-pati-o/jars"
jars = [
    f"{jars_path}/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    f"{jars_path}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    f"{jars_path}/kafka-clients-3.4.1.jar",
    f"{jars_path}/commons-pool2-2.11.1.jar",
    f"{jars_path}/lz4-java-1.8.0.jar",
    f"{jars_path}/snappy-java-1.1.10.3.jar",
    f"{jars_path}/slf4j-api-2.0.7.jar",
    f"{jars_path}/jsr305-3.0.2.jar",
    f"{jars_path}/hadoop-client-api-3.3.4.jar",
    f"{jars_path}/hadoop-client-runtime-3.3.4.jar"
]

spark = SparkSession.builder \
    .appName("TransitoStreaming") \
    .master("local[2]") \
    .config("spark.jars", ",".join(jars)) \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType()),
    StructField("vehiculo", StringType()),
    StructField("velocidad", IntegerType()),
    StructField("lugar", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transito") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print(">>> Streaming OK - Abre http://localhost:4040 <<<")
query.awaitTermination()
