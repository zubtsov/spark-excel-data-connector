package org.apache.spark.excel

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderFromIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class ExcelPartitionReaderFactory(
                                        sqlConf: SQLConf,
                                        broadcastedConf: Broadcast[SerializableConfiguration],
                                        dataSchema: StructType,
                                        readDataSchema: StructType,
                                        partitionSchema: StructType,
                                        filters: Seq[Filter]) extends FilePartitionReaderFactory {
  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    new PartitionReaderFromIterator(???)
  }
}
