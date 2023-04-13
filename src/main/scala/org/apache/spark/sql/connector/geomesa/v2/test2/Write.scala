package org.apache.spark.sql.connector.geomesa.v2.test2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

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
  println("partition id =" + partitionId)

  override def write(record: InternalRow): Unit = {

    println("write")
    println("record = " + record)

  }

  override def commit(): WriterCommitMessage = {
    println("commit")
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = println("close")
}