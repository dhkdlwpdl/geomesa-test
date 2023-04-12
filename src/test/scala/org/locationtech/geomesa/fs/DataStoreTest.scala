package org.locationtech.geomesa.fs

import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.util.Collections
import scala.collection.JavaConverters.mapAsJavaMapConverter

object DataStoreTest {
  private val sftName = "asdasdasd"
  private val sftSpec = "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled=s2:geom"
  private val formatString = "parquet"

  def main(args: Array[String]): Unit = {
    val gzip = "<configuration><property><name>parquet.compression</name><value>gzip</value></property></configuration>"

    val dsParams = Map(
      "fs.path" -> "D:\\IdeaProjects\\Test-Project\\geomesa-test\\fs",
      "fs.encoding" -> formatString,
      "fs.config.xml" -> gzip)
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]
    val sft = SimpleFeatureTypes.createType(sftName, sftSpec)
    ConfigurationUtils.setScheme(sft, "daily", Collections.singletonMap("dtg-attribute", "dtg"))

    // Test1: Create Schema 되는지 확인
    ds.createSchema(sft)

    // Test2: 원래 Entry를 기억하냐 못하냐
    println(ds.getEntry(new NameImpl(sftName)))
    println(ds.getEntry(new NameImpl(sftName)).getTypeName)
    val fs = ds.getFeatureSource(sftName)
    println(ds.getEntry(new NameImpl(sftName)))

    ds.getFeatureWriter(sftName, Transaction.AUTO_COMMIT)


  }
}
