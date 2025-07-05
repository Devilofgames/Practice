import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

val schema = StructType(Array(
  StructField("Name", StringType, true),
  StructField("Age", IntegerType, true),
  StructField("City", StringType, true)
))

val data = Seq(
  Row("Alice", 30, "New York"),
  Row("Bob", 24, "London"),
  Row("Charlie", 35, "Paris"),
  Row("David", 29, "Tokyo"),
  Row("Eve", 42, "Sydney"),
  Row("Frank", 27, "Berlin")
)

val rdd = spark.sparkContext.parallelize(data)
val df = spark.createDataFrame(rdd, schema)

println("DataFrame created:")
df.show()

val outputPath = "F:/spark_output/my_first_parquet_data"

df.write.mode("overwrite").parquet(outputPath)

println(s"DataFrame successfully written to Parquet at: $outputPath")