package org.apache.spark.sql.connector.text

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class TextDataSourceV2 extends TableProvider with DataSourceRegister with RelationProvider {

  val schm = StructType(Seq(StructField("value", StringType)))

  override def shortName(): String = "yjv2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = schm

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new TextTable(properties, schema)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new TextRelation(parameters.asJava, schm, sqlContext)

}