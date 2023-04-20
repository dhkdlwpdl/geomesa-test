package org.apache.spark.sql.connector

import org.apache.spark.sql.SparkSession
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.opengis.feature.simple.SimpleFeature

import java.util.Collections
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.Random

object SparkSQLTestUtils {
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.ui.enabled", value = false)
      .master("local[4]")
      .getOrCreate()
  }

  val random = new Random()
  random.setSeed(0)

//  val ChiSpec = "arrest:String,case_number:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326"
  val ChiSpec = "fid:Integer,name:String,double:Double,long:Long,float:Float,bool:Boolean,date:Date,timestamp:Timestamp," +
    "*point:Point,linestring:LineString,polygon:Polygon,multipoint:MultiPoint,multilinestring:MultiLineString,multipolygon:MultiPolygon," +
    "geometrycollection:GeometryCollection,geometry:Geometry,liststring:List[String],mapintString:Map[Integer,String],bytes:Bytes"
  val ChicagoSpec = SimpleFeatureTypes.createType("chicago", ChiSpec)

  def ingestChicago(ds: DataStore): Unit = {
    val sft = ChicagoSpec

    // Chicago data ingest
    ds.createSchema(sft)

    val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

    val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

    val f = List(
      ScalaSimpleFeature.create(sft, "some-id", "123",
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
//      ScalaSimpleFeature.create(sft, "1", "true", "1", "2016-01-01T00:00:00.000Z", createPoint(new Coordinate(-76.5, 38.5))),
//      ScalaSimpleFeature.create(sft, "2", "true", "2", "2016-01-02T00:00:00.000Z", createPoint(new Coordinate(-77.0, 38.0))),
//      ScalaSimpleFeature.create(sft, "3", "true", "3", "2016-01-03T00:00:00.000Z", createPoint(new Coordinate(-78.0, 39.0)))
    )

    f.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))

    fs.addFeatures(DataUtilities.collection(f.map(_.asInstanceOf[SimpleFeature]).asJava))
  }

  def ingestPoints(ds: DataStore,
                   name: String,
                   points: Map[String, String]): Unit = {
    val sft = SimpleFeatureTypes.createType(
      name, "name:String,*geom:Point:srid=4326")
    ds.createSchema(sft)

    val features = DataUtilities.collection(points.map(x => {
      new ScalaSimpleFeature(sft, x._1, Array(x._1, WKTUtils.read(x._2).asInstanceOf[Point])).asInstanceOf[SimpleFeature]
    }).toList.asJava)

    val fs = ds.getFeatureSource(name).asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(features)
  }

  def ingestGeometries(ds: DataStore,
                       name: String,
                       geoms: Map[String, String]): Unit = {
    val sft = SimpleFeatureTypes.createType(
      name, "name:String,*geom:Geometry:srid=4326")
    ds.createSchema(sft)

    val features = DataUtilities.collection(geoms.map(x => {
      new ScalaSimpleFeature(sft, x._1, Array(x._1, WKTUtils.read(x._2))).asInstanceOf[SimpleFeature]
    }).toList.asJava)

    val fs = ds.getFeatureSource(name).asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(features)
  }

  def generatePoints(gf: GeometryFactory, numPoints: Int): Map[String, String] = {
    (1 until numPoints).map { i =>
      val x = -180 + 360 * random.nextDouble()
      val y = -90 + 180 * random.nextDouble()
      (i.toString, gf.createPoint(new Coordinate(x, y)).toText)
    }.toMap
  }

  def generatePolys(gf: GeometryFactory, numPoints: Int): Map[String, String] = {
    (1 until numPoints).map { i =>
      val x = -180 + 360 * random.nextDouble()
      val y = -90 + 180 * random.nextDouble()
      val width = (3 * random.nextDouble()) / 2.0
      val height = (1 * random.nextDouble()) / 2.0
      val (minX, maxX, minY, maxY) = (x - width, x + width, y - height, y + height)
      val coords = Array(new Coordinate(minX, minY), new Coordinate(minX, maxY),
        new Coordinate(maxX, minY), new Coordinate(maxX, maxY), new Coordinate(minX, minY))
      (i.toString, gf.createPolygon(coords).toText)
    }.toMap
  }
}
