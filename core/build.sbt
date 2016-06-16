libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "0.6.0",
  "com.chuusai" %% "shapeless" % "2.3.2-SNAPSHOT",
  "co.fs2" %% "fs2-core" % "0.9.0-M2",
  "co.fs2" %% "fs2-cats" % "0.1.0-M2",
  "com.typesafe" % "config" % "1.3.0")

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4" % Test

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21" % Test
