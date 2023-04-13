package org.apache.spark.sql.connector.geomesa.v2.test2.case1

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.connector.geomesa.v2.common.GeoMesaSparkSQL.GEOMESA_SQL_FEATURE
import org.apache.spark.sql.connector.geomesa.v2.common.SparkUtils
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.{StringType, StructType}
import org.geotools.data.{DataStoreFinder, Transaction}
import org.locationtech.geomesa.utils.geotools.FeatureUtils

import java.util

class GeoMesaWriteBuilder(options: util.Map[String, String], schema: StructType) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new GeoMesaBatchWrite(options, schema)
}

class GeoMesaBatchWrite(options: util.Map[String, String], schema: StructType) extends BatchWrite {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = new GeoMesaDataWriterFactory(options, schema)
}

class GeoMesaDataWriterFactory(options: util.Map[String, String], schema: StructType) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = new GeoMesaDataWriter(partitionId, options, schema)
}

class GeoMesaDataWriter(partitionId: Int, options: util.Map[String, String], schema: StructType) extends DataWriter[InternalRow] {
  private val typeName = options.get(GEOMESA_SQL_FEATURE)
  private val ds = DataStoreFinder.getDataStore(options)
  private val fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)

  override def write(record: InternalRow): Unit = {
    // InternalRow -> SF 변환 로직 작성 필요. 임시로 InternalRow -> Row로 변환하도록 작성
    val row = new GenericRowWithSchema(record.toSeq(schema).toArray, schema)

    val mappings = SparkUtils.rowsToFeaturesByYJ(typeName, schema)
    val sf = mappings.apply(row)


    println(sf, fw)
    FeatureUtils.write(fw, sf, true)
  }

  override def commit(): WriterCommitMessage = {
    println("commit")
    // 여기서 flush를 하면 될것같은데
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
//    fw.close()
    ds.dispose()
  }
}