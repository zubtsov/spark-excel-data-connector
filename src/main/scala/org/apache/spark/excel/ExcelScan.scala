package org.apache.spark.excel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import scala.collection.JavaConverters._

case class ExcelScan(
                      sparkSession: SparkSession,
                      fileIndex: PartitioningAwareFileIndex,
                      dataSchema: StructType,
                      readDataSchema: StructType,
                      readPartitionSchema: StructType,
                      options: CaseInsensitiveStringMap,
                      pushedFilters: Array[Filter],
                      partitionFilters: Seq[Expression] = Seq.empty,
                      dataFilters: Seq[Expression] = Seq.empty
                    ) extends FileScan {
  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    ExcelPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }
}
