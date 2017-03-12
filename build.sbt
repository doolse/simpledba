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
  scalaVersion := "2.12.1",
  resolvers += Resolver.sonatypeRepo("snapshots")
)

val subSettings = Seq(
  name := "simpledba-" + baseDirectory.value.getName,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-s", "10"),
  scalacOptions += "-Ypartial-unification",
  scalacOptions ++= Option("-Xlog-implicits").filter(_ => config.getBoolean("debug.implicits")).toSeq,
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
) ++ commonSettings

lazy val coreDep = core % "test->test;compile->compile"
lazy val dynamodb = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val cassandra = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val jdbc = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val circe = project.settings(subSettings: _*).dependsOn(core)
lazy val core = project.settings(subSettings: _*)

lazy val parent = (project in file(".")).aggregate(core, cassandra, dynamodb, circe, jdbc)

commonSettings