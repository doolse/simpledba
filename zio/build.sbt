val ZioVersion = "1.0.0-RC11-1"
val ZioJava = "1.1.0.0-RC2"
val ZioCats = "2.0.0.0-RC2"
val ZioGroup = "dev.zio"

libraryDependencies ++= Seq("zio",
                            "zio-streams",
                            ).map(ZioGroup %% _ % ZioVersion)

libraryDependencies ++= Seq(
  ZioGroup %% "zio-interop-cats" % ZioCats,
  ZioGroup %% "zio-interop-java" % ZioJava
)
