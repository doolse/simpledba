import com.typesafe.config.ConfigFactory
import sbt.Keys._
import sbt.Resolver

name := "simpledba"

lazy val config = ConfigFactory.parseFile(file(".") / "project/reference.conf")

val commonSettings = Seq(
  organization := "io.doolse",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  scalacOptions ++= Option("-Xlog-implicits").filter(_ => config.getBoolean("debug.implicits")).toSeq,
  resolvers += Resolver.sonatypeRepo("snapshots")
)

val subSettings = Seq(
  name := "simpledba-" + baseDirectory.value.getName,
  libraryDependencies += "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.0-RC3" % Test,
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.7.1"),
  addCompilerPlugin("com.milessabin" % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full)
) ++ commonSettings

lazy val coreDep = core % "test->test;compile->compile"
lazy val dynamodb = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val cassandra = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val circe = project.settings(subSettings: _*).dependsOn(core)
lazy val core = project.settings(subSettings: _*)

lazy val parent = (project in file(".")).aggregate(core, cassandra, dynamodb, circe)

commonSettings