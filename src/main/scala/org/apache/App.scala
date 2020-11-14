package org.apache

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val session = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    session.read.format("xlsx")
      .load("C:\\Users\\zubtsov\\IdeaProjects\\spark-excel-data-connector\\TestBook.xlsx")
      .show()
  }

}
