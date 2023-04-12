package org.apache.spark.sql.connector.text

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.{BufferedReader, File, FileReader}
import java.util

class TextScanBuilder(options: util.Map[String, String], schema: StructType) extends ScanBuilder {
  private val path = options.get("path")

  override def build(): Scan = new TextScan(schema, path)
}

class TextScan(schema: StructType, path: String) extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = new TextBatch(path)
}

class TextBatch(path: String) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    // path 디렉토리의 파일 목록을 읽어옵니다.
    val files = new File(path.replace("file:/", "")).listFiles().filter(_.isFile)
    // 파일마다 하나의 파티션을 생성합니다.
    files.map(file => TextFileInputPartition(file.getAbsoluteFile))
  }

  override def createReaderFactory(): PartitionReaderFactory = new TextPartitionReaderFactory
}
case class TextFileInputPartition(filePath: File) extends InputPartition

class TextPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new TextPartitionReader(new BufferedReader(new FileReader(partition.asInstanceOf[TextFileInputPartition].filePath)))
  }
}

class TextPartitionReader(reader: BufferedReader) extends PartitionReader[InternalRow] {
  private var line: String = _

  override def next(): Boolean = {
    line = reader.readLine()
    line != null
  }

  override def get(): InternalRow = InternalRow(UTF8String.fromString(line))

  override def close(): Unit = reader.close()

}