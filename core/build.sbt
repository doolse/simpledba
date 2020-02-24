libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3",
  "org.typelevel" %% "cats-core" % "2.0.0",
  "org.typelevel" %% "cats-effect" % "2.0.0",
  "com.typesafe" % "config" % "1.3.0")

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.3" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.21" % Test)