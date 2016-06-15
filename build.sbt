import sbt.Keys._
import sbt.Resolver

name := "simpledba"

val commonSettings = Seq(
  organization := "io.doolse",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  scalacOptions += "-Xlog-implicits",
  resolvers += Resolver.sonatypeRepo("snapshots")
)
val subSettings = Seq(
  name := "simpledba-" + baseDirectory.value.getName,
  libraryDependencies += "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.0-RC3" % Test,
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.7.1"),
  addCompilerPlugin("com.milessabin" % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full)
) ++ commonSettings

lazy val dynamodb = project.settings(subSettings: _*).dependsOn(core)
lazy val cassandra = project.settings(subSettings: _*).dependsOn(core)
lazy val circe = project.settings(subSettings: _*).dependsOn(core)
lazy val core = project.settings(subSettings: _*)

lazy val parent = (project in file(".")).aggregate(core, cassandra, dynamodb, circe)

commonSettings