package org.apache.spark.sql.connector.geomesa.v2.test1

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.util

class GeoMesaWriteBuilder(options: util.Map[String, String]) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new GeoMesaBatchWrite(options)
}

class GeoMesaBatchWrite(options: util.Map[String, String]) extends BatchWrite {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = new GeoMesaDataWriterFactory(options)
}

class GeoMesaDataWriterFactory(options: util.Map[String, String]) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = new GeoMesaDataWriter(partitionId, options)
}

class GeoMesaDataWriter(partitionId: Int, options: util.Map[String, String]) extends DataWriter[InternalRow] {
  val filePath: String = options.get("path").replace("file:/", "") + s"/part-$partitionId.txt"
  val writer: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, true))) // true: Append 모드로 파일을 연다

  override def write(record: InternalRow): Unit = {
    val rowString = record.getString(0)
    writer.write(rowString)
    writer.newLine()
  }

  override def commit(): WriterCommitMessage = {
    writer.flush()
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = writer.close()
}