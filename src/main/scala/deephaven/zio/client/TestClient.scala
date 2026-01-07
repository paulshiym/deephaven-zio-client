package deephaven.zio.client

import java.time.LocalTime

import zio._
import zio.stream._
import zio.logging.LogFormat
import zio.logging.backend.SLF4J

final case class TickData(symbol: String, time: LocalTime, value: Double)

object TestClient extends ZIOAppDefault {
  private val symbols = Vector("AAPL", "MSFT", "GOOG", "AMZN", "TSLA")

  private val tickStream: ZStream[Any, Throwable, TickData] =
    ZStream
      .repeatZIO {
        for {
          idx <- Random.nextIntBounded(symbols.size)
          value <- Random.nextDouble
          now <- Clock.localDateTime.map(_.toLocalTime)
        } yield TickData(symbols(idx), now, value * 100.0)
      }
      .schedule(Schedule.spaced(200.millis))
      .take(25)

  override val bootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j(LogFormat.colored)

  override def run: ZIO[Any, Throwable, Unit] =
    for {
      _ <- ZIO.logInfo("Connecting to Deephaven at localhost:10000")
      authHeader <- sys.env.get("DEEPHAVEN_AUTH") match {
        case Some(value) if value.trim.nonEmpty =>
          ZIO.succeed(value.trim)
        case _ =>
          ZIO
            .fromOption(sys.env.get("DEEPHAVEN_PSK"))
            .orElseFail(new IllegalArgumentException("DEEPHAVEN_PSK is not set"))
            .map(psk => s"psk $psk")
      }
      _ <- DeephavenService.publish(
        tableName = "ticks",
        stream = tickStream,
        batchSize = 10,
        mode = UpdateMode.AppendOnly
      ).provide(DeephavenService.remote("localhost", 10000, authHeader))
      _ <- ZIO.logInfo("Published ticks to table: ticks")
    } yield ()
}
