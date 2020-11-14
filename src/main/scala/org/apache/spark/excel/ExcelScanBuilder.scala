package org.apache.spark.excel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ExcelScanBuilder(
                             sparkSession: SparkSession,
                             fileIndex: PartitioningAwareFileIndex,
                             schema: StructType,
                             dataSchema: StructType,
                             options: CaseInsensitiveStringMap
                           )
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {
  override def build(): Scan = {
    ExcelScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      Array.empty)
  }
}
