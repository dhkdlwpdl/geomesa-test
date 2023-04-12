package org.apache.spark.sql.connector.geomesa.v2.test1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import java.util

class GeoMesaRelation(options: util.Map[String, String], schm: StructType, sQLContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sQLContext

  override def schema: StructType = schm

//  override def insert(data: DataFrame, overwrite: Boolean): Unit =
//    data.write.format("geov2").options(options).mode(SaveMode.Append).save()

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val df = sqlContext.sparkSession.read.format("geov2").options(options).load()
    df.rdd
  }
}
