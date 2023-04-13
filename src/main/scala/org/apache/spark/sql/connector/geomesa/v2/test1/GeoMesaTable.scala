package org.apache.spark.sql.connector.geomesa.v2.test1

import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.geomesa.v2.common.GeoMesaSparkSQL.GEOMESA_SQL_FEATURE
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Set

class GeoMesaTable(options: util.Map[String, String], schema: StructType) extends SupportsRead {

  override def name(): String = options.get(GEOMESA_SQL_FEATURE)

  override def schema(): StructType = schema

  val caps: Set[TableCapability] = new util.HashSet[TableCapability]()
  caps.add(TableCapability.BATCH_READ)
  caps.add(TableCapability.BATCH_WRITE)

  // Spark 3.0에서 추가된 유니크한 특징
    // 해당 테이블이 어떤 종류의 Operation을 지원할것인지를 나타냄
    // Spark이 Operation 수행 전에 값을 통해서 사전 확인할 수 있음
  override def capabilities(): util.Set[TableCapability] = caps

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new GeoMesaScanBuilder(options, schema, name())
  }

}