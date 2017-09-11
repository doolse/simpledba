libraryDependencies += "com.typesafe" % "config" % "1.3.0"

logLevel := Level.Warn

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")