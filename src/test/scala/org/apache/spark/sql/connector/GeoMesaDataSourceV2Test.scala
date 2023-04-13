package org.apache.spark.sql.connector

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.spark.SparkSQLTestUtils

import java.util.{Map => JMap}
import scala.collection.JavaConverters.mapAsJavaMapConverter

object GeoMesaDataSourceV2Test {
  def main(args: Array[String]): Unit = {
    // DataStore 구성
    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true").asJava
    val ds: DataStore = DataStoreFinder.getDataStore(dsParams)

    // 테이블 구성
    SparkSQLTestUtils.ingestChicago(ds)

    // Spark Session 구성
    val spark: SparkSession = SparkSQLTestUtils.createSparkSession()
    spark.sparkContext.setLogLevel("ERROR")


    // 조회 테스트
      // 테스트1: DataFrame load
    val df: DataFrame = spark.read
      .format("geov2")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    df.show()

//      // 테스트2: Spark SQL select
//    spark.sql("create table test2 using geov2 options (cqengine 'true', geotools 'true', geomesa.feature 'chicago')")
//    spark.sql("show tables").show()
//    spark.sql("describe test2").show()
//    spark.sql("select * from test2").show()
//
//      // 테스트3: Spark SQL + Condition
//    spark.sql("select * from test2 where case_number = 3").show()


    // ------------------------------------------------------------------------------------------------

    // 적재 테스트
      // 테스트1: DataFrame save/saveAsTable
//    val dfWriter = df.write
//      .format("geov2")
//      .options(dsParams)
//      .option("geomesa.feature", "chicago")
//    dfWriter.save("D:/tmp/")
//    dfWriter.saveAsTable("test1")
//    spark.sql("show tables").show()
//    spark.sql("select * from test1").show()
//
//    // 테스트2: Spark SQL insert
//    spark.sql(s"""insert into test2
//                 | select '5' as __fid__,
//                 | 'yjyj' as arrest,
//                 | 123123 as case_number,
//                 | current_timestamp() as dtg,
//                 | st_point (10.0, 20.0) as geom""".stripMargin)



  }
}
