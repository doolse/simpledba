import com.typesafe.config.ConfigFactory
import sbt.Keys._
import sbt.Resolver

name := "simpledba"

lazy val prjDir = file("project")
lazy val config = ConfigFactory.parseFile(prjDir / "application.conf")
  .withFallback(ConfigFactory.parseFile(prjDir / "reference.conf"))

val commonSettings = Seq(
  organization := "io.doolse",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  resolvers += Resolver.sonatypeRepo("snapshots")
)

val subSettings = Seq(
  name := "simpledba-" + baseDirectory.value.getName,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-s", "10"),
  scalacOptions ++= Option("-Xlog-implicits").filter(_ => config.getBoolean("debug.implicits")).toSeq,
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