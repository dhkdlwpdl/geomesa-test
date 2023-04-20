package org.apache.spark.sql.connector

import org.apache.spark.sql.connector.geomesa.v2.common.SparkUtils
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.util.Collections
import scala.Array

object SparkUtilsTest {
  def main(args: Array[String]): Unit = {
//    val spec = "fid:Integer,name:String,double:Double,long:Long,float:Float,bool:Boolean,uuid:UUID,date:Date,timestamp:Timestamp,*point:Point,linestring:LineString,polygon:Polygon,multipoint:MultiPoint,multilinestring:MultiLineString,multipolygon:MultiPolygon,geometrycollection:GeometryCollection,geometry:Geometry,liststring:List[String],mapintString:Map[Integer,String],bytes:Bytes"

    val spec = "fid:Integer,name:String,double:Double,long:Long,float:Float,bool:Boolean,date:Date,timestamp:Timestamp,*point:Point,linestring:LineString,polygon:Polygon,multipoint:MultiPoint,multilinestring:MultiLineString,multipolygon:MultiPolygon,geometrycollection:GeometryCollection,geometry:Geometry,liststring:List[String],mapintString:Map[Integer,String],bytes:Bytes"
    val sft = SimpleFeatureTypes.createType("sftT03", spec)

    val sf = ScalaSimpleFeature.create(sft, "some-id", "123",
      "ehehhehhehhehehe",
      "169769313486231570e+308",
      "9223372036854775",
      "-2.402932347e+38",
      "true",
      //        "123e4567-e89b-12d3-a456-556642440000",
      //        "2023-02-20 16:03:15.000.000Z",
      //        "2050-01-01T00:00:00.000Z",
      "2016-01-01T00:00:00.000Z",
      "2016-01-01T00:00:00.000Z",
      "POINT(10.0 20.1)",
      "LINESTRING (30 10, 10 30, 40 40)",
      "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
      "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
      "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
      "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
      "GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))",
      "POINT(10 20)",
      Collections.singletonList("test"),
      Collections.singletonMap(1, "mapTest"),
      Array[Byte](0, 1))

    println(sf.getAttribute(9).getClass)

    val schema = SparkUtils.createStructType(sft)





    println(schema)
    val requiredColumns = schema.map(_.name).toArray // temporary
    val extractors = SparkUtils.getExtractors(requiredColumns, schema)

    val internalRow = SparkUtils.sf2InternalRowByYJ(sf, extractors)

    println(internalRow)
  }
}
