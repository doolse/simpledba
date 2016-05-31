libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "0.6.0",
  "com.chuusai" %% "shapeless" % "2.3.2-SNAPSHOT",
  "co.fs2" %% "fs2-core" % "0.9.0-SNAPSHOT",
  "co.fs2" %% "fs2-cats" % "0.1.0-SNAPSHOT",
  "org.typelevel" %% "kittens" % "1.0.0-M3")

scalacOptions += "-Xlog-implicits"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4" % Test
