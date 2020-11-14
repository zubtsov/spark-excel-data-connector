package org.apache.spark.excel

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ExcelDataSourceV2 extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = ???

  override protected def getTable(options: CaseInsensitiveStringMap): Table = ???

  override def shortName(): String = "xlsx"
}
