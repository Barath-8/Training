package org.parquet

object App {

  def main(args : Array[String]) {
    println( "Hello World!" )
    val par = new FileHandling
    par.writer()
    println("Writing Executed")
    par.reader()
    println("ReadingExecuted")

    println("new Branch")

  }

}
