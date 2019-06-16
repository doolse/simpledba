import com.typesafe.config.ConfigFactory
import sbt.Keys._
import sbt.{Resolver, addCompilerPlugin}

name := "simpledba"

lazy val prjDir = file("project")
lazy val config = ConfigFactory.parseFile(prjDir / "application.conf")
  .withFallback(ConfigFactory.parseFile(prjDir / "reference.conf"))

val commonSettings = Seq(
  organization := "io.github.doolse",
  version := "0.1.10-SNAPSHOT",
  scalaVersion := "2.12.8",
  resolvers += Resolver.sonatypeRepo("snapshots"),

  licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php")),

  homepage := Some(url("https://github.com/doolse/simpledba")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/doolse/simpledba"),
      "scm:git@github.com:doolse/simpledba.git"
    )
  ),
  developers := List(
    Developer(
      id = "doolse",
      name = "Jolse Maginnis",
      email = "doolse@gmail.com",
      url = url("https://github.com/doolse/simpledba")
    )
  ),
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
)

parallelExecution in ThisBuild := false

val subSettings = Seq(
  name := "simpledba-" + baseDirectory.value.getName,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-s", "10"),
  scalacOptions += "-Ypartial-unification",
  scalacOptions ++= Seq("-P:splain:implicits:true", "-P:splain:color:false"),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3"),
  addCompilerPlugin("io.tryp" % "splain" % "0.4.1" cross CrossVersion.patch)
) ++ commonSettings

lazy val core = project.settings(subSettings: _*)
lazy val coreDep = core % "test->test;compile->compile"
lazy val fs2 = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val fs2Test = fs2 % "test->test"
lazy val zio = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val zioTest = zio % "test->test"
lazy val dynamodb = project.settings(subSettings: _*).dependsOn(coreDep,fs2Test, zioTest)
//lazy val cassandra = project.settings(subSettings: _*).dependsOn(coreDep)
lazy val jdbc = project.settings(subSettings: _*).dependsOn(coreDep, fs2Test, zioTest)
lazy val circe = project.settings(subSettings: _*).dependsOn(coreDep)

lazy val parent = (project in file(".")).aggregate(core, fs2, jdbc, dynamodb, circe, zio)

commonSettings
