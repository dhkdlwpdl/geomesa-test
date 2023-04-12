package org.apache.spark.sql.connector.geomesa.v2

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.{Partition, SparkContext}
import org.geotools.data.Query
import org.locationtech.geomesa.spark.{GeoMesaSpark, SpatialRDDProvider}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

class GeoMesaScanBuilder(options: CaseInsensitiveStringMap, schema: StructType, name: String) extends ScanBuilder {
  override def build(): Scan = new GeoMesaScan(options, schema, name)
}

class GeoMesaScan(options: CaseInsensitiveStringMap, schema: StructType, name: String) extends Scan {
  // 테이블이 지원하는 schema 메서드와 겹치는 것처럼 보일 수 있지만, 컬럼 pruning이나 다른 최적화 작업 이후에 스키마가 달라질 수도 있고 스키마에 대한 추론 작업이 필요할 수도 있기 때문
  // Table 의 schema 메서드는 초기의 스키마를 반환하고 이 메서드는 실제 스키마를 반환함
  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val conf: Configuration = new Configuration(spark.sqlContext.sparkContext.hadoopConfiguration)
    //    val filt = ECQL.toFilter("")
    //    val requiredAttributes = requiredAttributes
    val query: Query = new Query(name)
    val spatialRDDProvider: SpatialRDDProvider = GeoMesaSpark(options)
    val spatialRDD = spatialRDDProvider.rdd(conf, spark.sparkContext, options.asScala.toMap, query)

    new GeoMesaBatch(options.asScala.toMap, name, spatialRDD.partitions, schema: StructType)
  }
}

class GeoMesaBatch(options: Map[String, String], name: String, partitions: Array[Partition], schema: StructType) extends Batch {
  // 데이터소스의 Batch 쿼리 Scan 의 물리적인 표현
  // 데이터가 얼마나 많은 파티션을 가지고 있는지와 어떻게 데이터를 파티션에서 읽어올건지 같은 물리적인 정보 제공을 위해 사용됨

  override def planInputPartitions(): Array[InputPartition] = {
    val parts = partitions.indices.map(value =>  GeoMesaInputPartition(value))
    parts.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new GeoMesaPartitionReaderFactory (options, name, schema)
}
case class GeoMesaInputPartition(value: Int) extends InputPartition

class GeoMesaPartitionReaderFactory(options: Map[String, String], name: String, schema: StructType) extends PartitionReaderFactory {
  // 실제 Data Reader를 생성하기 위한 Factory 클래스
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new GeoMeseaPartitionReader(options, name, partition, schema)
  }
}

class GeoMeseaPartitionReader(options: Map[String, String], name: String, partition: InputPartition, schema: StructType) extends PartitionReader[InternalRow] {
  private var iterator: Iterator[SimpleFeature] = null
  private val sparkContext: SparkContext = SparkContext.getOrCreate()

  override def next(): Boolean = {
    if (iterator == null ){
      val conf: Configuration = new Configuration(sparkContext.hadoopConfiguration)
      val query: Query = new Query(name)
      val spatialRDDProvider: SpatialRDDProvider = GeoMesaSpark(options.asJava)
      val spatialRDD = spatialRDDProvider.rdd(conf, sparkContext, options, query)
      val part = spatialRDD.partitions(partition.asInstanceOf[GeoMesaInputPartition].value)
      iterator = spatialRDD.iterator(part, org.apache.spark.TaskContext.get())
    }
    iterator.hasNext
  }

  override def get(): InternalRow = {
    val sf = iterator.next()

    val requiredColumns = schema.map(_.name).toArray // temporary
    val extractors = SparkUtils.getExtractors(requiredColumns, schema)
    SparkUtils.sf2InternalRowByYJ(sf, extractors) // SF -> InternalRow 로직 구현 필요
  }

  override def close(): Unit = println("read finished")

}