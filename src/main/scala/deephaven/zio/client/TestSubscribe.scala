package deephaven.zio.client

import zio._
import zio.stream._
import zio.logging.LogFormat
import zio.logging.backend.SLF4J

/**
  * Manual smoke test for remote live subscription.
  *
  * Usage:
  *   export DEEPHAVEN_PSK=...
  *   export JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
  *   sbt "runMain deephaven.zio.client.TestSubscribe"
  */
object TestSubscribe extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j(LogFormat.colored)

  override def run: ZIO[Any, Throwable, Unit] =
    for {
      authHeader <- ZIO
        .fromOption(sys.env.get("DEEPHAVEN_PSK").map(_.trim).filter(_.nonEmpty))
        .orElseFail(new IllegalArgumentException("DEEPHAVEN_PSK is not set"))
        .map(psk => s"io.deephaven.authentication.psk.PskAuthenticationHandler $psk")

      _ <- ZIO.logInfo("Subscribing to table 'ticks'...")

      _ <- DeephavenService
        .subscribe[TickData]("ticks")
        .tap(row => ZIO.logInfo(s"tick: $row"))
        .runDrain
        .provide(DeephavenService.remote("localhost", 10000, authHeader))
    } yield ()
}
