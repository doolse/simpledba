val ZioVersion = "1.0.0-RC8-12"
val ZioGroup = "dev.zio"

libraryDependencies ++= Seq("zio",
                            "zio-streams",
                            "zio-interop-cats",
                            "zio-interop-java").map(ZioGroup %% _ % ZioVersion)
