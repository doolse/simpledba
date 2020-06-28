val ZioVersion = "1.0.0-RC21-1"
val ZioJava = "1.1.0.0-RC6"
val ZioCats = "2.1.3.0-RC16"
val ZioGroup = "dev.zio"

libraryDependencies ++= Seq("zio",
                            "zio-streams",
                            ).map(ZioGroup %% _ % ZioVersion)

libraryDependencies ++= Seq(
  ZioGroup %% "zio-interop-cats" % ZioCats,
  ZioGroup %% "zio-interop-java" % ZioJava
)
