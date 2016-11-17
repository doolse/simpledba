libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "0.8.1",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "co.fs2" %% "fs2-core" % "0.9.2",
  "co.fs2" %% "fs2-cats" % "0.2.0",
  "com.typesafe" % "config" % "1.3.0")

libraryDependencies ++= Seq("org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.3" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.21" % Test)

