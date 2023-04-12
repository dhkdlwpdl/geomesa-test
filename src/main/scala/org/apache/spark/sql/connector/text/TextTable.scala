package org.apache.spark.sql.connector.text

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Set

class TextTable(options: util.Map[String, String], schema: StructType) extends SupportsRead with SupportsWrite {

  override def name(): String = options.get("table")

  override def schema(): StructType = schema

  val caps: Set[TableCapability] = new util.HashSet[TableCapability]()
  caps.add(TableCapability.BATCH_READ)
  caps.add(TableCapability.BATCH_WRITE)

  override def capabilities(): util.Set[TableCapability] = caps

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new TextScanBuilder(options, schema)
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new TextWriteBuilder(options)

}