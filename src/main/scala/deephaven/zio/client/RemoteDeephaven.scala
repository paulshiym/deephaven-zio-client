package deephaven.zio.client

import java.net.URI
import java.util.Collections
import java.util.concurrent.Executors

import io.deephaven.client.impl.{ClientConfig, ExportId, FlightDescriptorHelper, SessionConfig, SessionFactoryConfig, SessionImpl, SessionMiddleware}
import io.deephaven.proto.DeephavenChannelImpl
import io.deephaven.proto.backplane.grpc.{AuthenticationConstantsRequest, ConfigValue}
import io.deephaven.client.impl.BarrageSession
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable
import io.deephaven.uri.DeephavenTarget
import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightGrpcUtilsExtension}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot

import deephaven.zio.arrow.{ArrowBatch, ArrowSchema}
import zio._
import zio.stream._
import scala.jdk.CollectionConverters._

final class RemoteDeephaven private (
    session: SessionImpl,
    flightClient: FlightClient,
    barrage: BarrageSession,
    allocator: BufferAllocator,
    exports: Ref[Chunk[ExportId]]
) extends DeephavenService {

  override def publish[A: ArrowSchema: DeephavenTableSchema](
      tableName: String,
      stream: ZStream[Any, Throwable, A],
      batchSize: Int,
      mode: UpdateMode
  ): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for {
        _ <- ZIO.when(batchSize <= 0)(ZIO.fail(new IllegalArgumentException(s"batchSize must be > 0, got $batchSize")))
        _ <- ZIO.unless(mode == UpdateMode.AppendOnly) {
          ZIO.fail(new UnsupportedOperationException(s"Remote publish currently supports AppendOnly only, got: $mode"))
        }

        // Create an append-only input table with a schema derived from A.
        tableHeader = DeephavenTableSchema[A].header
        inputSpec: io.deephaven.qst.table.InputTable = InMemoryAppendOnlyInputTable.of(tableHeader)
        inputTable <- ZIO.attemptBlocking(session.serial().of(inputSpec))
        _ <- ZIO.attemptBlocking(inputTable.await())
        _ <- ZIO.attemptBlocking(session.publish(tableName, inputTable).get())

        _ <- stream
          .grouped(batchSize)
          .runForeach { chunk =>
            ZIO.attemptBlocking {
              // Upload the batch as a temporary table via Arrow Flight
              val exportId = session.newExportId()
              val descriptor = FlightDescriptorHelper.descriptor(exportId)
              val root = ArrowBatch.encodeRows(allocator, chunk)
              try {
                val out = flightClient.startPut(descriptor, root, new AsyncPutListener())
                out.putNext()
                root.clear()
                out.completed()
                out.getResult()
              } finally {
                root.close()
              }

              // Append the uploaded rows into the input table.
              session.addToInputTable(inputTable, exportId).get()

              // Release the temporary export.
              session.release(exportId).get()
              ()
            }
          }
          .ensuring(ZIO.attemptBlocking(inputTable.close()).ignore)
      } yield ()
    }

  override def subscribe[A: ArrowSchema: DeephavenRowDecoder](tableName: String): ZStream[Any, Throwable, A] =
    ZStream.scoped {
      for {
        hub <- Hub.unbounded[A]
        // Resolve the published table by name from the server query scope.
        // NOTE: `ticket(String)` expects a Deephaven ticket string, not a published name.
        handle <- ZIO.attemptBlocking {
          val spec = io.deephaven.qst.table.TicketTable.fromQueryScopeField(tableName)
          session.serial().of(spec)
        }
        _ <- ZIO.attemptBlocking(handle.await())

        options = BarrageSubscriptionOptions.builder().build()
        sub <- ZIO.attemptBlocking(barrage.subscribe(handle, options))
        table <- ZIO.attemptBlocking(sub.entireTable().get())

        // Emit initial snapshot (existing rows)
        _ <- ZIO.attemptBlocking {
          val it = table.getRowSet().iterator()
          while (it.hasNext) {
            val key = it.nextLong()
            Unsafe.unsafe { implicit u =>
              Runtime.default.unsafe.run(hub.publish(DeephavenRowDecoder[A].decode(table, key))).getOrThrowFiberFailure()
            }
          }
        }.forkDaemon

        // Subscribe to live updates (append-only expected; we emit added rows)
        _ <- ZIO.attemptBlocking {
          table.addUpdateListener(new io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter("zio-barrage-subscribe", table, false) {
            override def onUpdate(update: io.deephaven.engine.table.TableUpdate): Unit = {
              val acquired = update.acquire()
              try {
                val it = acquired.added().iterator()
                while (it.hasNext) {
                  val key = it.nextLong()
                  val a = DeephavenRowDecoder[A].decode(table, key)
                  Unsafe.unsafe { implicit u =>
                    Runtime.default.unsafe.run(hub.publish(a)).getOrThrowFiberFailure()
                  }
                }
              } finally {
                acquired.release()
              }
            }
          })
        }

        stream = ZStream.fromHub(hub)
        // Ensure resources released
        _ <- ZIO.addFinalizer(ZIO.attemptBlocking(handle.close()).ignore)
        _ <- ZIO.addFinalizer(hub.shutdown.ignore)
      } yield stream
    }.flatten

  def close(): UIO[Unit] =
    for {
      exportIds <- exports.get
      _ <- ZIO.foreachDiscard(exportIds)(exportId => ZIO.attempt(session.release(exportId).get()).ignore)
    } yield ()
}

object RemoteDeephaven {
  def layer(host: String, port: Int, psk: String): ZLayer[Any, Throwable, DeephavenService] =
    ZLayer.scoped {
      for {
        allocator <- ZIO.acquireRelease(ZIO.attempt(new RootAllocator(Long.MaxValue)))(alloc => ZIO.attempt(alloc.close()).ignore)
        scheduler <- ZIO.acquireRelease(ZIO.attempt(Executors.newSingleThreadScheduledExecutor()))(exec => ZIO.attempt(exec.shutdown()).ignore)
        clientConfig <- ZIO.attempt {
          ClientConfig.builder()
            .target(DeephavenTarget.of(URI.create(s"dh+plain://$host:$port")))
            .build()
        }
        sessionConfig <- ZIO.attempt {
          val authValue = Option(psk).map(_.trim).filter(_.nonEmpty).map { value =>
            if (value.contains(" ")) value
            else if (value.matches("(?i)^(psk|bearer|basic):.+")) value
            else s"Bearer $value"
          }
          val builder = SessionConfig.builder().scheduler(scheduler)
          authValue.foreach(builder.authenticationTypeAndValue)
          builder.build()
        }
        _ <- ZIO.foreachDiscard(Option(psk).map(_.trim).filter(_.nonEmpty)) { raw =>
          val normalized =
            if (raw.contains(" ")) raw
            else if (raw.matches("(?i)^(psk|bearer|basic):.+")) raw
            else s"Bearer $raw"
          val scheme =
            if (normalized.contains(" ")) normalized.takeWhile(!_.isWhitespace)
            else normalized.takeWhile(_ != ':')
          val token =
            if (raw.contains(" ")) raw.dropWhile(!_.isWhitespace).trim
            else if (raw.matches("(?i)^(psk|bearer|basic):.+")) raw.dropWhile(_ != ':').drop(1)
            else raw
          val tokenTail =
            if (token.length >= 2) token.takeRight(2)
            else "<redacted>"
          ZIO.logInfo(s"Auth header prepared: scheme=$scheme, length=${normalized.length}, tokenTail=$tokenTail")
        }
        factoryConfig <- ZIO.attempt {
          SessionFactoryConfig.builder()
            .clientConfig(clientConfig)
            .sessionConfig(sessionConfig)
            .scheduler(scheduler)
            .build()
        }
        factory <- ZIO.succeed(factoryConfig.factory())
        authConstants <- ZIO.attemptBlocking {
          val channel = new DeephavenChannelImpl(factory.managedChannel())
          val response = channel.configBlocking().getAuthenticationConstants(AuthenticationConstantsRequest.getDefaultInstance())
          response.getConfigValuesMap
        }.catchAll { e =>
          val message = Option(e.getMessage).getOrElse("no message")
          ZIO.logWarning(s"Failed to read Deephaven authentication constants: ${e.getClass.getSimpleName}: $message")
            .as(Collections.emptyMap[String, ConfigValue]())
        }
        _ <- ZIO.logInfo {
          val formatted = authConstants.asScala.toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"$k=${v.getStringValue}" }
            .mkString(", ")
          s"Deephaven authentication constants: [$formatted]"
        }
        session <- ZIO.acquireRelease(ZIO.attempt(factory.newSession().asInstanceOf[SessionImpl]))(sess => ZIO.attempt(sess.close()).ignore)
        flightClient <- ZIO.acquireRelease(ZIO.attempt {
          FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(
            allocator,
            factory.managedChannel(),
            Collections.singletonList(new SessionMiddleware(session))
          )
        })(client => ZIO.attempt(client.close()).ignore)
        barrage <- ZIO.acquireRelease(
          ZIO.attempt(BarrageSession.of(session, allocator, factory.managedChannel()))
        )(b => ZIO.attempt(b.close()).ignore)
        exports <- Ref.make(Chunk.empty[ExportId])
        service = new RemoteDeephaven(session, flightClient, barrage, allocator, exports)
        _ <- ZIO.addFinalizer(service.close())
        _ <- ZIO.addFinalizer(ZIO.attempt(factory.managedChannel().shutdown()).ignore)
      } yield service
    }
}
