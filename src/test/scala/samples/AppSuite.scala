package samples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class AppSuite extends FunSuite {
  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def generateColumnCombinations(cols: Seq[String], combSize: Int): Seq[Seq[String]] = {
    if (combSize == 1)
      cols.map(Seq(_))
    else
      ???
  }

  def identifyPotentialPrimaryKeys(df: DataFrame, maxPKSize: Int) = {
    import org.apache.spark.sql.functions._

    val cols = df.columns
    if (maxPKSize > cols.size) throw new Exception("The size of composite primary key can't be greater than the number of columns!")

    val combs = (1 to maxPKSize).map(currentPKSize => {
      generateColumnCombinations(cols, currentPKSize)
    }).reduce(_ ++ _)

    df.groupBy().agg(count(lit(1)).as("rows"),
      combs.map(c => countDistinct(lit(1), c.map(col): _*).as(c.mkString(","))): _*)
  }

  test("Identifying primary key") {
    val testData = Seq(
      (1, "str1", 1.1),
      (2, "str2", 2.2),
      (1, "str3", 3.3)
    ).toDF("integer_col", "string_col", "double_col")

    identifyPotentialPrimaryKeys(testData, 1).show()
  }
}
