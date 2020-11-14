package org.apache.spark.sql

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.excel.{ExcelScan, ExcelScanBuilder}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{AtomicType, DataType, StructType, UserDefinedType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ExcelTable(
                                       name: String,
                                       sparkSession: SparkSession,
                                       options: CaseInsensitiveStringMap,
                                       paths: Seq[String],
                                       userSpecifiedSchema: Option[StructType],
                                       fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = ExcelScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

    override def inferSchema(files: Seq[FileStatus]): Option[StructType] = throw new Exception("Not supported yet")

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???

    override def supportsDataType(dataType: DataType): Boolean = dataType match {
      case _: AtomicType => true

      case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

      case _ => false
    }

    override def formatName: String = "XLSX"
  }
