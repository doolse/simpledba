libraryDependencies ++=
  Seq (
    "org.postgresql" % "postgresql" % "42.2.1",
    "org.hsqldb" % "hsqldb" % "2.4.0"
  ).map(_ % Test)