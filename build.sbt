import sbt.Keys._
import sbt.Resolver

name := "SimpleAccess"

val commonSettings = Seq(
  organization := "io.doolse",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.8",
  resolvers += Resolver.sonatypeRepo("snapshots")
)
val subSettings = Seq(
  name := "SimpleAccess-" + baseDirectory.value.getName,
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.7.1")
) ++ commonSettings

lazy val dynamodb = project.settings(subSettings: _*).dependsOn(core)
lazy val cassandra = project.settings(subSettings: _*).dependsOn(core)
lazy val core = project.settings(subSettings: _*)

lazy val parent = (project in file(".")).aggregate(dynamodb)

commonSettings