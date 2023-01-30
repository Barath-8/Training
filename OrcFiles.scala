
import com.nimbusds.jose.util.StandardCharset
import org.apache.commons.net.ntp.TimeStamp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hive.orc.{OrcFile, Reader, TypeDescription}

import java.time.LocalDate

class OrcFiles {
  def reader(): Unit = {
    val conf = new Configuration
    val path = new Path("OrcFile.orc")
    val reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))
    val batch = reader.getSchema.createRowBatch()

    val name = batch.cols(0).asInstanceOf[BytesColumnVector]
    val age = batch.cols(1).asInstanceOf[LongColumnVector]
    val mobile = batch.cols(2).asInstanceOf[LongColumnVector]
    val percentage = batch.cols(3).asInstanceOf[DoubleColumnVector]
    val good = batch.cols(4).asInstanceOf[LongColumnVector]
    val time = batch.cols(5).asInstanceOf[TimestampColumnVector]
    val date  = batch.cols(6).asInstanceOf[LongColumnVector]

    println(name.vector.length)

    val op = new Reader.Options
    op.schema(reader.getSchema)
    val rows = reader.rows(op)

//    /*
    while(rows.nextBatch(batch)){
      for(r <- 0 until batch.size){
        val aRow = if(age.isRepeating) 0 else r
        val mRow = if(mobile.isRepeating) 0 else r
        val pRow = if(percentage.isRepeating) 0 else r
        val gRow = if(percentage.isRepeating) 0 else r
        val tRow = if(time.isRepeating) 0 else r
        val dRow = if(date.isRepeating) 0 else r

        println(
            name.toString(r) +"\t" +
            (if(age.noNulls || !age.isNull(aRow)) age.vector(aRow) else null) +"\t" +
            (if (mobile.noNulls || !mobile.isNull(mRow)) mobile.vector(mRow) else null) +"\t" +
            (if (percentage.noNulls || !percentage.isNull(pRow)) percentage.vector(pRow) else null) +"\t" +
            (if (good.noNulls || !good.isNull(gRow)) good.vector(gRow) else null ) +"\t"+
            (if (time.noNulls || !time.isNull(gRow)) time.time(gRow) else null )+"\t"+
            (if(date.noNulls || !date.isNull(dRow)) date.vector(gRow) else null)
        )
        val ti = if (time.noNulls || !time.isNull(tRow)) time.time(tRow) else 0
        val times = TimeStamp.getNtpTime(ti)
        println(times.getDate)

        val da = if(date.noNulls || !date.isNull(dRow)) date.vector(gRow) else 0
        val dates = LocalDate.ofEpochDay(da)
        println(dates+"\n")
      }
    }
//     */
    println(reader.getSchema)
    println("Total columns "+batch.numCols)
    println("Compression kind " +reader.getCompressionKind)
    println("Compressed Size " +reader.getCompressionSize)
    println("Raw Size " +reader.getRawDataSize)
    println("Max size per batch " + batch.getMaxSize)
    reader.getStripes.forEach(st => println(st.getLength + "\t" + st.getDataLength + "\t" + st.getFooterLength + "\t"
      + st.getIndexLength + "\t" + st.getNumberOfRows + "\t" + st.getOffset))


    println("No. of Stripes : " + reader.getFileTail.getFooter.getStripesCount)

    reader.getOrcProtoStripeStatistics.forEach(stats => println(stats.toString))

    reader.getStripeStatistics.forEach(stats => stats.toString)

    batch.cols.foreach(q => println(q.getClass))


  }


  def writer(): Unit = {
    val conf = new Configuration
    val path = new Path("OrcFile.orc")
    val schema = TypeDescription.fromString("struct<name:string,age:int,mobile:bigint,"+
                                            "percentage:double,good:boolean,time:timestamp,date:date>")

    val writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema))

    val batch = schema.createRowBatch()

    val name = batch.cols(0).asInstanceOf[BytesColumnVector]
    val age = batch.cols(1).asInstanceOf[LongColumnVector]
    val mobile = batch.cols(2).asInstanceOf[LongColumnVector]
    val percentage = batch.cols(3).asInstanceOf[DoubleColumnVector]
    val good = batch.cols(4).asInstanceOf[LongColumnVector]
    val time = batch.cols(5).asInstanceOf[TimestampColumnVector]
    val date = batch.cols(6).asInstanceOf[LongColumnVector]

    println(batch.getMaxSize)
    val choice = Array(1,2,3,4,5,6,7)
    var idx = 0

    val list = Array("barath",21,8072274534L,82.33,true)

    for (_ <- 1 to 20000){
      val row = {
        batch.size += 1;
        batch.size - 1
      }

      name.setVal(row,list(0).asInstanceOf[String].getBytes(StandardCharset.UTF_8))
      age.vector(row) = list(1).asInstanceOf[Int]
      mobile.vector(row) = list(2).asInstanceOf[Long]
      percentage.vector(row) = list(3).asInstanceOf[Double]
      good.vector(row) = 1000
      time.time(row) = TimeStamp.getCurrentTime.getTime
      date.vector(row) = LocalDate.now().toEpochDay

      choice(idx) match {
        case 1 =>
          name.isNull(row)=true
          name.noNulls = false
        case 2 =>
          age.isNull(row) = true
          age.noNulls = false
        case 3 =>
          mobile.isNull(row) = true
          mobile.noNulls = false
        case 4 =>
          percentage.isNull(row) = true
          percentage.noNulls = false
        case 5 =>
          good.isNull(row) = true
          good.noNulls = false
        case 6 =>
          time.isNull(row) = true
          time.noNulls = false
        case 7 =>
          date.isNull(row) = true
          date.noNulls = false
      }

      idx += 1
      if ( idx == 7 ) idx=0

      if(batch.size==batch.getMaxSize){
        percentage.fillWithNulls()
        writer.addRowBatch(batch)
        batch.reset()
      }
    }

    if (batch.size != 0) writer.addRowBatch(batch)

    writer.close()

  }
}
