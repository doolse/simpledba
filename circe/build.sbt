val circeVersion = "0.9.3"
libraryDependencies ++= Seq (
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)