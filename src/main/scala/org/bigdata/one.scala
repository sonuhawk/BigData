import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, DataFrame}

// Create a SparkSession
val spark: SparkSession = SparkSession.builder()
  .appName("FirstRowInEachGroup")
  .getOrCreate()

// Sample DataFrame with two columns: "group" and "value"
val data = Seq(
  (1, "A"),
  (1, "B"),
  (2, "C"),
  (2, "D"),
  (2, "E")
)

val df: DataFrame = spark.createDataFrame(data).toDF("group", "value")

// Define a window specification over the "group" column
val windowSpec = Window.partitionBy("group").orderBy("value")

// Use the row_number function to assign a row number to each row within the partition
val rankedDF = df.withColumn("row_number", row_number().over(windowSpec))

// Filter rows where row_number is 1 to get the first row of each group
val resultDF = rankedDF.filter(col("row_number") === 1)

// Show the result
resultDF.show()
