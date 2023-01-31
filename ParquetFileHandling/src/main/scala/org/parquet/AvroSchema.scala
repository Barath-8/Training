package org.parquet

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData

class UserPojo {
  private var name :String =""
  private var age : Int = 0
  private var mobile : Long =0
  private var good : Boolean = false
  private var percent :Double = 0

  def getName: String = name
  def getAge: Int = age
  def getMobile : Long = mobile
  def getGood : Boolean = good
  def getPercent : Double = percent

  def setName(name : String): Unit = this.name=name

  def setAge(age : Int): Unit = this.age=age

  def setMobile(mobile : Long): Unit = this.mobile=mobile

  def setGood(good : Boolean): Unit = this.good=good

  def setPercent(percent : Double): Unit = this.percent=percent

}

class AvroSchema {
  def createSchemaOfPojo(): Schema = {
    val schema = ReflectData.get().getSchema(classOf[UserPojo])
    println(schema.toString(true))

    val newSchema = new Schema.Parser().parse(schema.toString.replaceAll(""""namespace":"org.parquet",""", ""))

    println(newSchema)
    newSchema
  }
}
