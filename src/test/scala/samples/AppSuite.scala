package samples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class AppSuite extends FunSuite {
  val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  def generateColumnCombinations(cols: Seq[String], combSize: Int) = {

  }

  def potentialPrimaryKeysStatistics(df: DataFrame, maxPKSize: Int) = {
    val cols = df.columns
    if (maxPKSize > cols.size) throw new Exception("The size of composite primary key can't be greater than the number of columns!")

    for (currentPKSize <- 1 to maxPKSize) {
      ???
    }
  }

  test("Identifying primary key") {
    val testData = Seq(
      (1, "str1", 1.1),
      (2, "str2", 2.2),
      (3, "str3", 3.3)
    ).toDF("integer_col", "string_col", "double_col")

    testData.show()
  }
}
