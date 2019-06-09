val ZioVersion = "1.0-RC5+30-fb9a9e2d"
val ZioGroup = "dev.zio"

libraryDependencies ++= Seq("zio",
                            "zio-streams",
                            "zio-interop-cats",
                            "zio-interop-java").map(ZioGroup %% _ % ZioVersion)
