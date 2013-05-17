package net.marchildon

import java.io.File
import java.text.SimpleDateFormat

/*
Nom de la Station|CAMPBELL RIVER A
Province|BRITISH COLUMBIA
Latitude|49,95
Longitude|-125,27
Altitude|108,80
Identification Climat|1021261
Identification OMM|
Identification TC|YBL
Toutes les heures sont exprimées en heure normale locale (HNL). Pour convertir l'heure locale en heure avancée de l'Est (HAE), ajoutez 1 heure s'il y a lieu.

Légende
M|Données Manquantes
E|Valeur Estimée
ND|Non Disponible
**|Données fournies par un partenaire, non assujetties à un révision par les Archives climatiques nationales du Canada

Date/Heure|Année|Mois|Jour|Heure|Qualité des Données|Temp (°C)|Temp Indicateur|Point de rosée (°C)|Point de rosée Indicateur|Hum. rel (%)|Hum. rel. Indicateur|Dir. du vent (10s deg)|Dir. du vent Indicateur|Vit. du vent (km/h)|Vit. du vent Indicateur|Visibilité (km)|Visibilité Indicateur|Pression à la station (kPa)|Pression à la station Indicateur|Hmdx|Hmdx Indicateur|Refroid. éolien|Refroid. éolien Indicateur|Temps
1966-06-01 00:00|1966|06|01|00:00| |||||||||||||||||||
...

Identification Climat
  indicatif climatologique, indicatif de station, numéro de station

  L'indicatif de station, ou indicatif climatologique ou numéro de station, est un nombre de 7 chiffres attribué par le Service météorologique du Canada à un site où sont prises des observations météorologiques officielles et qui constitue un identificateur permanent et unique.

  Le premier chiffre identifie la province où est située la station, le deuxième et le troisième la zone climatologique à l'intérieur de la province.

  Lorsqu'on cesse de prendre des observations à un site, le numéro n'est pas réaffecté à des stations subséquentes (qui peuvent ou non avoir des noms différents) à moins qu'on estime que les enregistrements des stations précédente et suivante peuvent être combinés à des fins de climatologie.

indicatif OMM

  Nombre de 5 chiffres attribué de façon permanente aux stations canadiennes par l'Organisation météorologique mondiale pour les identifier à l'échelle internationale. L'indicatif de l'OMM est un identificateur international attribué par le Service météorologique du Canada conformément aux normes de l'OMM aux stations qui transmettent des observations en formats météorologiques internationaux en temps réel.

Identification TC:	YUL
*/

case class Attribute(description: String, value: String)

object Export extends App {
  val dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  println("Starting Export")
  //new File(".").listFiles.filter(_.isDirectory).take(10).map(exportStation(_))
  readCsv(new File("51157/fre-hourly-2013-03.csv"))

  private def parseDate(date: String) = {
    try {
      Some(dateParser.parse(date))
    } catch {
      case e: java.text.ParseException => None
    }
  }

  private def exportStation(dir: File) : Int = {
    println("Station: " + dir.getName)
    dir.listFiles.map(readCsv(_))
    return 0
  }

  def toTsd(record: List[String], country: String, province: String, stationName: String) = record match {
    case List(key, value) => Some(Attribute(key, value))
    case List(dateStr, y, m, d, h, quality, values @ _*) => {
      parseDate(dateStr) match {
        case Some(date) => {
          val ts = date.getTime
          val tags = Seq(
            "station=" + stationName.replaceAll(" ", "_"),
            "country=" + country,
            "province=" + province,
            "quality=" + quality).mkString(" ")
          val string = Some(identity[String] _)
          val ignore = None
          val extractors = List(
            ("air.temp", string),
            ("air.temp.ind", ignore),
            ("dew.temp", string),
            ("dew.temp.ind", ignore),
            ("humidity", string),
            ("humidity.ind", ignore),
            ("wind.speed", string),
            ("wind.speed.ind", ignore),
            ("visibility", string),
            ("visibility.ind", ignore),
            ("pressure", string),
            ("pressure.ind", ignore),
            ("humidex", string),
            ("humidex.ind", ignore),
            ("wind.chill", string),
            ("weather", ignore)
          )
          Some(values.zip(extractors)
            .flatMap { case ((value), (name, extractor)) => extractor.map(f => f(value)).map(v => (name, v)) }
            .map { case (name, value) => name + " " + ts + " " + value + " " + tags})
        }
        case _ => None
      }
    }
    case _ => None
  }
//    record match {
//      case List(dateStr, _, _, _, _, quality, temp, _, dewTemp, _, humidity, _, windDir, _, windSpeed, _, visibility, _, pressure, _, humidex, _, windFactor, weather) => {
//        val ts = dateParser.parse(dateStr).getTime
//        val tags = "country=CA province=UNKNOWN city=UNKNOWN"
//        List(
//          ("temp.air", temp),
//          ("temp.dew", dewTemp)
//            ("humidity", humidity),
//          ("wind.speed", windSpeed),
//          ("visibility", visibility),
//          ("pressure", pressure),
//          ("humidex", humidex),
//          ("wind.chill", windFactor)
//        ).map match {
//          case (metric, value) => (metric, ts, value, tags).mkString(" ")
//        }
//      }
//      case _ => "(not matched)"

  private def readCsv(file: File) = {
    println("Reading " + file)
    // Note: Parsing one line at a time even if CSV parser can parse all lines at once.
    var country = "Canada"
    var province = "Unknown"
    var station = "Unknown"
    for (line <- scala.io.Source.fromFile(file, "latin1").getLines) {
      toTsd(CSV.parseRecord(line), country, province, station) match {
        case Some(Attribute("Nom de la Station", name)) => { station = name }
        case Some(Attribute("Province", name)) => { province = name}
        case Some(datapoints : List[String]) => println(datapoints)
        case other @ _ => {println("Error: " + other)}
      }
      //toTsd(record).map(println(_.join("\n"))) // TODO
      //println(toTsd(record))
    }
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
