libraryDependencies ++=
  Seq("org.postgresql" % "postgresql" % "42.2.1")
    .map(_ % Test)