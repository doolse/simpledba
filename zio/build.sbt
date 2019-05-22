val ZioVersion = "1.0-RC4"

libraryDependencies ++= Seq("org.scalaz" %% "scalaz-zio",
                            "org.scalaz" %% "scalaz-zio-streams",
                            "org.scalaz" %% "scalaz-zio-interop-cats",
                            "org.scalaz" %% "scalaz-zio-interop-java").map(_ % ZioVersion)
