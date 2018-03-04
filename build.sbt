name := "simpledba2"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3",
  "co.fs2" %% "fs2-core" % "0.10.2",
  "org.postgresql" % "postgresql" % "42.2.1"
)

scalacOptions += "-Ypartial-unification"

scalacOptions += "-P:splain:implicits:true"

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
addCompilerPlugin("io.tryp" % "splain" % "0.2.7" cross CrossVersion.patch)

