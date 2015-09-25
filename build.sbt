import _root_.sbt.Keys._

name := "HelloSpark"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" %"provided"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
