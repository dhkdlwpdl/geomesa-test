package org.apache.spark.sql.connector

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions.{ST_PointFromText, constructorNames}
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper.nullableUDF
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.{GeometryCollection, Point}

import java.util.{Map => JMap}
import scala.collection.JavaConverters.mapAsJavaMapConverter

object GeoMesaDataSourceV2WithoutSRPTest {
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
      .format("geov2withoutsrp")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    df.show()

      // 테스트2: Spark SQL select
    spark.sql("create table test2 using geov2withoutsrp options (cqengine 'true', geotools 'true', geomesa.feature 'chicago')")
    spark.sql("show tables").show()
    spark.sql("describe test2").show()
    spark.sql("select * from test2").show()

      // 테스트3: Spark SQL + Condition
    spark.sql("select * from test2 where fid = 123").show()


    println("------------------------------------------------------------------------------------------------")
    // 적재 테스트
      // 테스트1: DataFrame save/saveAsTable
    val dfWriter = df.write
      .format("geov2withoutsrp")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
        .mode(SaveMode.Append)

    dfWriter.save("D:/tmp/")
//    dfWriter.saveAsTable("test1")
    spark.sql("show tables").show()
//    spark.sql("select * from test1").show()

    // 테스트2: Spark SQL insert
    spark.sql("select * from test2").show()

    spark.sql("select * from test2").printSchema()

    val ST_GeometryCollectionFromText: String => GeometryCollection = nullableUDF(text => WKTUtils.read(text).asInstanceOf[GeometryCollection])
    spark.sqlContext.udf.register("ST_GeometryCollectionFromText", ST_GeometryCollectionFromText)


    spark.sql(s"""insert into test2
                 | select '5' as __fid__,
                 | 5 as fid,
                 | 'yjyj' as name,
                 | 123123 as double,
                 | 123123 as long,
                 | 1.2 as float,
                 | true as bool,
                 | current_timestamp() as date,
                 | current_timestamp() as timestamp,
                 | ST_PointFromText ('POINT (30 10)') as point,
                 | ST_LineFromText ('LINESTRING (30 10, 10 30, 40 40)') as linestring,
                 | ST_PolygonFromText ('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') as polygon,
                 | ST_MPointFromText ('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))') as multipoint,
                 | ST_MLineFromText ('MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))') as multilinestring,
                 | ST_MPolyFromText ('MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))') as multipolygon,
                 | ST_GeometryCollectionFromText ('GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))') as geometrycollection,
                 | ST_GeomFromWKT ('POINT (30 10)') as geometry,
                 | array('a','b','c') as liststring,
                 | MAP(1, 'hehe', 2, 'hehehe', 3, 'hehehehe') as mapstring,
                 | binary(12345) as bytes
                 | """.stripMargin)

    spark.sql("select * from test2").show()


  }
}
