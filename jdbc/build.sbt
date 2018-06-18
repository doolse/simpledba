libraryDependencies ++=
  Seq (
    "org.hsqldb" % "hsqldb" % "2.4.0",
    "org.postgresql" % "postgresql" % "42.2.1",
    "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8"
  ).map(_ % Test)