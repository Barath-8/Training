package org.parquet
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}

import java.io.{FileNotFoundException, IOException}

class FileHandling {

  def writer(): Unit = {

    try {

      val schema = new AvroSchema().createSchemaOfPojo()

      val writer = AvroParquetWriter
        .builder[GenericRecord](new Path("parquetFile.parquet"))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

      val record = new GenericData.Record(schema)

      var choose = 1

      def convertToRecord(user : UserPojo): Unit = {
        record.put("name",user.getName)
        record.put("age",user.getAge)
        record.put("good",user.getGood)
        record.put("mobile",user.getMobile)
        record.put("percent",user.getPercent)

//        choose match {
//          case 1 => record.put("name",null)
//          case 2 => record.put("age",null)
//          case 3 => record.put("good",null)
//          case 4 => record.put("mobile",null)
//          case 5 => record.put("percent",null)
//          case _ => choose = 1
//        }
      }

      for (_ <- 0 to 100) {

        val user = new UserPojo
        user.setName("Barath")
        user.setAge(21)
        user.setMobile(8072274534L)
        user.setGood(true)
        user.setPercent(82.311)

//        choose += 1
        convertToRecord(user)
        writer.write(record)
      }
      writer.close()

    }
    catch {
      case io : IOException =>
        println("Error Writing the file")
        io.printStackTrace()
      case e : Exception =>
        println("Other Exception")
        e.printStackTrace()
    }
  }


  def reader(): Unit = {
    try {
      val conf = new Configuration()
      conf.set(AvroReadSupport.READ_INT96_AS_FIXED, "true")

      //  val path = new Path("/Users/barath-16320/Downloads/userdata2.parquet")
      val path = new Path("parquetFile.parquet")

      val reader = AvroParquetReader
        .builder[GenericRecord](path)
        .withConf(conf)
        .build()

      val read = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))

      println(read.getFileMetaData.toString)
      println(read.getFooter.toString)
      println(read.getRecordCount)

      val rowGroups = read.getRowGroups

      for (row <- 0 until (rowGroups.size())) {
        val rowGroup = rowGroups.get(row)
        val columnChunkMetaData = rowGroup.getColumns
        columnChunkMetaData.forEach(col => println(col))
      }

      var record = reader.read()
      println(record.getSchema.toString(true))

      while (record != null) {
        println(record)
        record = reader.read()
      }
      reader.close()
    }
    catch {
      case fnf: FileNotFoundException =>
        println("File mission on the given path")
        fnf.printStackTrace()
      case io: IOException =>
        println("Error Reading the file")
        io.printStackTrace()
      case e: Exception =>
        println("\n\nOther Exception")
        e.printStackTrace()
    }
  }

}