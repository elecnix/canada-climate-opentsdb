package net.marchildon

import org.junit._
import java.io.File
import Export.Attribute
import Export.toTsd
import java.io.StringWriter
import scala.io.Source

class SimpleTest {

  @Test
  def export() {
    import Export._

    val country = "Canada"
    var province = "Unknown"
    var station = "Unknown"

    val writer = new StringWriter

    using(Source.fromURL(this.getClass.getResource("/input.csv"), "latin1")) { input =>

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

}