package net.marchildon

import java.io.{File,FileWriter}
import java.text.SimpleDateFormat

/**

Reads files like this:

<pre>
Nom de la Station|CAMPBELL RIVER A
Province|BRITISH COLUMBIA
Latitude|49,95
Longitude|-125,27
Altitude|108,80
Identification Climat|1021261
Identification OMM|
Identification TC|YBL
Toutes les heures sont exprim��es en heure normale locale (HNL). Pour convertir l'heure locale en heure avanc��e de l'Est (HAE), ajoutez 1 heure s'il y a lieu.

L��gende
M|Donn��es Manquantes
E|Valeur Estim��e
ND|Non Disponible
**|Donn��es fournies par un partenaire, non assujetties �� un r��vision par les Archives climatiques nationales du Canada

Date/Heure|Ann��e|Mois|Jour|Heure|Qualit�� des Donn��es|Temp (��C)|Temp Indicateur|Point de ros��e (��C)|Point de ros��e Indicateur|Hum. rel (%)|Hum. rel. Indicateur|Dir. du vent (10s deg)|Dir. du vent Indicateur|Vit. du vent (km/h)|Vit. du vent Indicateur|Visibilit�� (km)|Visibilit�� Indicateur|Pression �� la station (kPa)|Pression �� la station Indicateur|Hmdx|Hmdx Indicateur|Refroid. ��olien|Refroid. ��olien Indicateur|Temps
1966-06-01 00:00|1966|06|01|00:00| |||||||||||||||||||
</pre>

Legend:
<blockquote>
Identification Climat
  indicatif climatologique, indicatif de station, num��ro de station

  L'indicatif de station, ou indicatif climatologique ou num��ro de station, est un nombre de 7 chiffres attribu�� par le
  Service m��t��orologique du Canada �� un site o�� sont prises des observations m��t��orologiques officielles et qui constitue
  un identificateur permanent et unique.

  Le premier chiffre identifie la province o�� est situ��e la station, le deuxi��me et le troisi��me la zone climatologique
  �� l'int��rieur de la province.

  Lorsqu'on cesse de prendre des observations �� un site, le num��ro n'est pas r��affect�� �� des stations subs��quentes (qui
  peuvent ou non avoir des noms diff��rents) �� moins qu'on estime que les enregistrements des stations pr��c��dente et
  suivante peuvent ��tre combin��s �� des fins de climatologie.

indicatif OMM

  Nombre de 5 chiffres attribu�� de fa��on permanente aux stations canadiennes par l'Organisation m��t��orologique mondiale
  pour les identifier �� l'��chelle internationale. L'indicatif de l'OMM est un identificateur international attribu�� par
  le Service m��t��orologique du Canada conform��ment aux normes de l'OMM aux stations qui transmettent des observations
  en formats m��t��orologiques internationaux en temps r��el.

Identification TC:	YUL
</blockquote>

Output is designed to be ingested by OpenTSDB. For example:

<pre>
air.temp 1362114000 20.0 station=MONTREAL_INTL country=Canada province=QUEBEC quality=PARTNER
<pre>

*/
object Export extends App {
  case class Attribute(description: String, value: String)
  val dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  
  //val float: PartialFunction[String, String] = { case s if !s.isEmpty =>  s.replaceAll(",", ".") }
  
  //val ignore: PartialFunction[String, String] = { case "asdf1234" =>  "" }
  
  type Extractor = String => Option[String]
  
  val float : Extractor = { case "" => None; case s  => Some(s.replaceAll(",", ".")) }
  
  val ignore: Extractor = { _ => None }
  
  val extractors = List(
    ("air.temp", float),
    ("air.temp.ind", ignore),
    ("dew.temp", float),
    ("dew.temp.ind", ignore),
    ("humidity", float),
    ("humidity.ind", ignore),
    ("wind.speed", float),
    ("wind.speed.ind", ignore),
    ("visibility", float),
    ("visibility.ind", ignore),
    ("pressure", float),
    ("pressure.ind", ignore),
    ("humidex", float),
    ("humidex.ind", ignore),
    ("wind.chill", float),
    ("weather", ignore)
  )

  Console.err.println("Starting Export")

  // TODO Pass file or directory as a parameter, and process files recursively
  //new File(".").listFiles.filter(_.isDirectory).map(exportStation(_))
  //  private def exportStation(dir: File) : Int = {
  //    Console.err.println("Station: " + dir.getName)
  //    dir.listFiles.map(readCsv(_))
  //    return 0
  //  }

  using (new FileWriter("metric-defs.tsd"))  { writer =>
    for ((name, ex) <- extractors; x <- ex(1.toString())) writer.write(name + "\n")
  }
  println("Created metric-defs.tsd. Metrics can be created like this:")
  println("for metric in `cat metric-defs.tsd` ; do tsdb mkmetric $metric ; done")

  val file = new File("../51157/fre-hourly-2013-03.csv")
  val outfileName = file.getAbsolutePath + ".tsd"
  using (new FileWriter(outfileName))  { writer =>
    Console.err.println("Reading " + file)
    Console.err.println("Writing " + outfileName)
    // Note: Parsing one line at a time even if CSV parser can parse all lines at once.
    val country = "Canada"
    var province = "Unknown"
    var station = "Unknown"

    for (line <- scala.io.Source.fromFile(file, "latin1").getLines) {
      toTsd(CSV.parseRecord(line), country, province, station) match {
        case Some(Attribute("Nom de la Station", name)) => { station = name }
        case Some(Attribute("Province", name)) => { province = name}
        case Some(datapoints : List[String]) => for (point <- datapoints) { writer.write(point); writer.write("\n") }
        case other @ _ => { Console.err.println("Ignored: " + other) }
      }
    }
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B = try { f(param) } finally { param.close() }

  private def parseDate(date: String) = {
    try {
      Some(dateParser.parse(date))
    } catch {
      case e: java.text.ParseException => None
    }
  }

  def toTsd(record: List[String], country: String, province: String, stationName: String) = record match {
    case List(key, value) => Some(Attribute(key, value))
    case List(dateStr, y, m, d, h, quality, values @ _*) => {
      parseDate(dateStr) match {
        case Some(date) => {
          val ts = date.getTime / 1000
          val tags = Seq(
            "station=" + stationName.replaceAll(" ", "_"),
            "country=" + country,
            "province=" + province,
            "quality=" + quality.replaceAllLiterally("**", "PARTNER")).mkString(" ")
          
          /*
          Some(values.zip(extractors)
            .map { case (value, (name, extractor)) if extractor.isDefinedAt(value) => (name, extractor(value)) }
            .map { case (name, value) => name + " " + ts + " " + value + " " + tags})
          */

           Some { for( (value, (name, extractor)) <- values.zip(extractors);
                x <- extractor(value) ) yield name + " " + ts + " " + x + " " + tags }
               
        }
        case _ => None
      }
    }
    case _ => None
  }
}

import scala.util.parsing.combinator._

object CSV extends RegexParsers {
  override val skipWhitespace = false

  def COMMA   = ","
  def DQUOTE  = "\""
  def DQUOTE2 = "\"\"" ^^ { case _ => "\"" }
  def CR      = "\r"
  def LF      = "\n"
  def CRLF    = "\r\n"
  def TXT     = "[^\",\r\n]".r

  def file: Parser[List[List[String]]] = repsep(record, CRLF) <~ opt(CRLF)
  def record: Parser[List[String]] = rep1sep(field, COMMA)
  def field: Parser[String] = (escaped|nonescaped)
  def escaped: Parser[String] = (DQUOTE~>((TXT|COMMA|CR|LF|DQUOTE2)*)<~DQUOTE) ^^ { case ls => ls.mkString("")}
  def nonescaped: Parser[String] = (TXT*) ^^ { case ls => ls.mkString("") }

  def parse(s: String) = parseAll(file, s) match {
    case Success(res, _) => res
    case _ => List[List[String]]()
  }

  def parseRecord(s: String) = parseAll(record, s) match {
    case Success(res, _) => res
    case _ => List[String]()
  }
}
