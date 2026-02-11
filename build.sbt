ThisBuild / scalaVersion := "2.13.18"
ThisBuild / organization := "deephaven.zio"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "deephaven-zio-client",
    libraryDependencies ++= {
      val zioVersion = "2.1.24"
      val zioLoggingVersion = "2.5.2"
      val shapelessVersion = "2.3.10"
      val arrowVersion = "18.0.0"
      val deephavenVersion = "0.39.4"

      val zioDeps = Seq(
        "dev.zio" %% "zio" % zioVersion,
        "dev.zio" %% "zio-streams" % zioVersion,
        "dev.zio" %% "zio-test" % zioVersion % Test,
        "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
        "dev.zio" %% "zio-logging" % zioLoggingVersion,
        "dev.zio" %% "zio-logging-slf4j" % zioLoggingVersion,
        "org.slf4j" % "slf4j-simple" % "2.0.13"
      )

      val shapelessDeps = Seq(
        "com.chuusai" %% "shapeless" % shapelessVersion
      )

      val arrowDeps = Seq(
        "org.apache.arrow" % "arrow-vector" % arrowVersion,
        "org.apache.arrow" % "arrow-memory-unsafe" % arrowVersion,
        "org.apache.arrow" % "arrow-memory-netty" % arrowVersion
      )

      val deephavenDeps = Seq(
        "io.deephaven" % "deephaven-java-client-session" % deephavenVersion,
        "io.deephaven" % "deephaven-java-client-flight" % deephavenVersion,
        "io.deephaven" % "deephaven-uri" % deephavenVersion
      )

      zioDeps ++ shapelessDeps ++ arrowDeps ++ deephavenDeps
    },
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Needed for Apache Arrow on Java 17+ (reflective access to java.nio.Buffer.address)
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    )
  )
