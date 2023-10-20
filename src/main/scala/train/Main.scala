package train
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("MySpark")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .master("local")
      .getOrCreate()

    val salesDF = readDF()

    val finalDF = salesDF.transform(groupSales)

    printDF(finalDF)
    spark.stop()
  }

  def readDF(
      path: String = "src/main/resources/sales.csv"
  )(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ",")
      .csv(path)

  def groupSales(df: DataFrame): DataFrame =
    df
      .groupBy(col("operation_date"), col("product_id"))
      .agg(
        count(col("product_id")).as("total_amount"),
        sum(col("price")).as("total_price")
      )

  def printDF(df: DataFrame)(implicit spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write
      .partitionBy("operation_date")
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("final")
  }
}
