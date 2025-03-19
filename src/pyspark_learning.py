from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, lit, count, avg, min, max, sum


spark = SparkSession.builder \
        .appName("MyPysparkLearnings") \
        .getOrCreate()

print(spark)


data = [("John", 30), ("Alice", 28), ("Bob", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, schema=columns)
df.show()

df1 = spark.read.csv("../data/INvideos.csv", header=True, inferSchema=True)
print(df1.describe())

df1.select("video_id", "title", "views", "likes", "comment_count").filter(col("comment_count") > 100).limit(10).show()
df1 = df1.withColumn("engagement_score", (col("likes") + col("dislikes") + col("views") + col("comment_count")).cast("int"))
df1.limit(10).show()
df1.select(max("engagement_score").alias("max_popular_video")).show()
df1.groupBy("category_id").agg(sum("views").alias("Total_category_video_views")).limit(10).show()

df1.orderBy(col("engagement_score").desc()).limit(10).show()
df1.select("video_id", "title", "views", "likes", "comment_count", "engagement_score").orderBy(col("engagement_score").desc()).limit(10).show()

# Read JSON file
df2 = spark.read.option("multiline", "true").json("../data/IN_category_id.json")

df_flattened = df2.withColumn("items", explode(col("items")))

# Show the flattened data after explode
df_flattened.limit(2).show(truncate=False)

df_category = df_flattened.select(
    col("items.kind").alias("items_kind"),
    col("items.etag").alias("item_etag"),
    col("items.id").alias("id"),
    col("items.snippet.channelId").alias("channelId"),
    col("items.snippet.title").alias("title"),
    col("items.snippet.assignable").alias("assignable")
)
# Show the flattened data
df_category.limit(2).show(truncate=False)


df1_c = df1.join(df_category, df1.category_id == df_category.id, "left") \
        .select(df1["*"], df_category["title"].alias("category_name"))

df1_c = df1_c.fillna({"category_name":"Unknown"})
df1_c.limit(5).show()

rdd = spark.sparkContext.parallelize(data)
df_rdd = rdd.toDF(columns)
df_rdd.show()