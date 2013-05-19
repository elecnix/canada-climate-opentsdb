import sbt._
import sbt.Keys._

object ClimatBuild extends Build {

  lazy val climat = Project(
    id = "climat",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "Climat",
      organization := "net.marchildon",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      // add other settings here
      libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "0.8.0",
      libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test"
    )
  )
}
