package org.apache.spark.sql.connector.text

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import java.util

class TextRelation(options: util.Map[String, String], schm: StructType, sQLContext: SQLContext)
  extends BaseRelation with InsertableRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sQLContext

  override def schema: StructType = schm

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.write.format("yjv2").options(options).mode(SaveMode.Append).save()

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val df = sqlContext.sparkSession.read.format("yjv2").options(options).load()
    df.rdd
  }
}
