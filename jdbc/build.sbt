libraryDependencies += "org.postgresql" % "postgresql" % "42.0.0" % Test
libraryDependencies += "org.hsqldb" % "hsqldb" % "2.3.4" % Test

parallelExecution in Test := false
