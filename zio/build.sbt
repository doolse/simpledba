val ZioVersion = "1.0.0-RC10-1"
val ZioJava = "1.1.0-RC1"
val ZioCats = "1.3.1.0-RC2"
val ZioGroup = "dev.zio"

libraryDependencies ++= Seq("zio",
                            "zio-streams",
                            ).map(ZioGroup %% _ % ZioVersion)

libraryDependencies ++= Seq(
  ZioGroup %% "zio-interop-cats" % ZioCats,
  ZioGroup %% "zio-interop-java" % ZioJava
)
