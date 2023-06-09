package org.apache.spark.sql.connector.geomesa.v2.test1

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.geomesa.v1.SQLTypes
import org.apache.spark.sql.connector.geomesa.v2.common.SparkUtils
import org.apache.spark.sql.connector.geomesa.v2.common.GeoMesaSparkSQL.GEOMESA_SQL_FEATURE
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.geotools.data.DataStore
import org.locationtech.geomesa.utils.io.WithStore

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * DataSource V2 테스트 w. SpatialRDDProvider
 */
class GeoMesaDataSource extends TableProvider with DataSourceRegister with RelationProvider {
  override def shortName(): String = "geov2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // 만약 스키마 추론이 필요한 경우 empty 스키마를 반환하면 된다고 되어있엇음 ,,

    val newFeatureName = options.get(GEOMESA_SQL_FEATURE)

    // 몰랴 걍 일단 무조건 DataStore에 저장되어있다고 쳐
    val storeSft = WithStore[DataStore](options) { ds =>
      if (ds.getTypeNames.contains(newFeatureName)) {
        ds.getSchema(newFeatureName)
      } else {
        throw new RuntimeException ("shhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh,,,")
      }
    }

    SparkUtils.createStructType(storeSft)
  }

  private val spark: SparkSession = SparkSession.builder().getOrCreate()

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    SQLTypes.init(spark.sqlContext)
    new GeoMesaTable(properties, schema)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val caseInsensitiveMap = new CaseInsensitiveStringMap(parameters.asJava)
    val schem = inferSchema(caseInsensitiveMap)

    new GeoMesaRelation(parameters.asJava, schem, sqlContext)
  }
}