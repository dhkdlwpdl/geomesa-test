package org.apache.spark.sql.connector

import java.util.{Map => JMap}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import java.nio.file.{Files, Path, Paths}

object TestDataSourceTest {
  def main(args: Array[String]): Unit = {
    // Spark Session 구성
    val spark: SparkSession = SparkSQLTestUtils.createSparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    // 파일 디렉토리 (없으면 만들기)
    val fileDir = s"D:/tmp/hehe + ${System.currentTimeMillis()}"
    val folderPath: Path = Paths.get(fileDir)
    if (!Files.exists(folderPath)) {
      Files.createDirectory(folderPath)
      println("Folder Created")
    } else {
      println("Folder Exists")
    }
    // 테이블이름
    val tableName = "hehe"
    // 데이터소스 파라미터 설정
    val dsParams: JMap[String, String] = Map("path" -> fileDir, "table" -> tableName).asJava

    // 테스트 1: Write
    import spark.implicits._
    val asd = spark.sparkContext.parallelize(Seq(("A"), ("B"),("C"),("AA"), ("BB"), ("CC"),("a"), ("b"), ("c"))).toDF("value")
    asd.write.format("yjv2").options(dsParams).mode("append").save()

    // 테스트 2: Read
    val df: DataFrame = spark.read
      .format("yjv2")
      .options(dsParams)
      .load()
    df.show()
    df.select("value").filter("value = 'a'").show()

    spark.sql(s"create table $tableName (value string) using yjv2 options (path '$fileDir', table '$tableName')")
    spark.sql("show tables").show()
    spark.sql(s"select * from $tableName where value = 'a'").show()

    spark.sql(s"insert into $tableName values ('y')")
    spark.sql(s"insert into $tableName values ('j')")
    spark.sql(s"insert into $tableName values ('h')")
    spark.sql(s"insert into $tableName values ('e')")
    spark.sql(s"insert into $tableName values ('e')")
    spark.sql(s"insert into $tableName values ('h')")
    spark.sql(s"insert into $tableName values ('e')")
    spark.sql(s"insert into $tableName values ('h')")
    spark.sql(s"select * from $tableName where value >= 'e'").show()


    //    df.count()

    //    df.write.saveAsTable("yj_hehe")

    //    df.write.format("yjv2").options(dsParams).saveAsTable(tableName)
  }

}
