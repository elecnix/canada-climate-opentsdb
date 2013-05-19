package net.marchildon

import org.junit._
import scala.io._
import java.io.{ File, StringWriter }
import Export._
import java.io.Reader

class SimpleTest {

  implicit val latin1 = Codec("latin1")

  @Test
  def export() {
    val country = "Canada"
    var province = "Unknown"
    var station = "Unknown"
    val writer = new StringWriter
    using(Source.fromURL(this.getClass.getResource("/input.csv"))) { input =>
      for (line <- input.getLines) {
        toTsd(CSV.parseRecord(line), country, province, station) match {
          case Some(Attribute("Nom de la Station", name)) => { station = name }
          case Some(Attribute("Province", name)) => { province = name }
          case Some(datapoints: List[String]) =>
            for (point <- datapoints) { writer.write(point); writer.write("\n") }
          case other @ _ => { Console.err.println("Ignored: " + other) }
        }
      }
    }
    val exported = writer.toString()
    println(exported)
  }

  @Test
  def scalaCSV() {
    import com.github.tototoshi.csv._
    implicit object CSVFormat extends DefaultCSVFormat {
      override val separator = '|'
    }
    def csv(reader: Reader) = CSVReader.open(reader)

    val country = "Canada"
    var province = "Unknown"
    var station = "Unknown"
    val writer = new StringWriter

    using(Source.fromURL(this.getClass.getResource("/input.csv"))) { input =>
      for (line <- csv(input.reader)) {
        toTsd(line.toList, country, province, station) match {
          case Some(Attribute("Nom de la Station", name)) => { station = name }
          case Some(Attribute("Province", name)) => { province = name }
          case Some(datapoints: List[String]) =>
            for (point <- datapoints) { writer.write(point); writer.write("\n") }
          case other @ _ => { Console.err.println("Ignored: " + other) }
        }
      }
    }
    val exported = writer.toString()
    println(exported)
  }

}