val circeVersion = "0.13.0"
libraryDependencies ++= Seq (
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)