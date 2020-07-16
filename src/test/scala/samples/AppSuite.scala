package samples

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class AppSuite extends FunSuite {
  val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  test("Identifying primary key") {
    val testData = Seq(
      (1, "str1", 1.1),
      (2, "str2", 2.2),
      (3, "str3", 3.3)
    ).toDF("integer_col", "string_col", "double_col")

    testData.show()
  }
}
