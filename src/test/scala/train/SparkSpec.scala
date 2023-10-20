package train
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import train.Main.{groupSales, readDF}

import java.time.LocalDate

class SparkSpec extends AnyFreeSpec with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  "DataFrame created" in {
    val expectedData = Seq(
      (1, LocalDate.parse("2020-01-01"), 12),
      (1, LocalDate.parse("2020-01-01"), 12),
      (2, LocalDate.parse("2020-01-01"), 5),
      (1, LocalDate.parse("2020-01-02"), 13),
      (2, LocalDate.parse("2020-01-02"), 14)
    )
    val expectedDF = expectedData.toDF("product_id", "operation_date", "price")

    val actualDF = readDF(spark)
    compareDF(actualDF, expectedDF).shouldBe(true)
  }

  "DataFrame grouped" in {
    val incomingDF = readDF(spark)
    val actualDF = incomingDF.transform(groupSales)
    val expectedData = Seq(
      (LocalDate.parse("2020-01-01"), 1, 2, 24),
      (LocalDate.parse("2020-01-01"), 2, 1, 5),
      (LocalDate.parse("2020-01-02"), 1, 1, 13),
      (LocalDate.parse("2020-01-02"), 2, 1, 14)
    )
    val expectedDF = expectedData.toDF(
      "operation_date",
      "product_id",
      "total_amount",
      "total_price"
    )
    compareDF(actualDF, expectedDF).shouldBe(true)
  }

  "DataFrame saved" in {
    val path =
      "./final/operation_date=2020-01-01/part-00000-18035db3-520d-4e87-a131-19d9fe960aeb.c000.csv"
    val actualDF = readDF(spark, path)
    val expectedData = Seq(
      (2, 1, 5),
      (1, 2, 24)
    )
    val expectedDF =
      expectedData.toDF("product_id", "total_amount", "total_price")
    compareDF(actualDF, expectedDF).shouldBe(true)
  }

  def compareDF(actualDF: DataFrame, expectedDF: DataFrame): Boolean = {
    val diff = actualDF
      .except(expectedDF)
      .union(expectedDF.except(actualDF))
    diff.count() == 0
  }

  override def afterAll(): Unit =
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
}
