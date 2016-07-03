libraryDependencies += "com.typesafe" % "config" % "1.3.0"

logLevel := Level.Warn

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")
