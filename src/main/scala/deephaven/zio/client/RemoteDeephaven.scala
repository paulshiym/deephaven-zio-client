package deephaven.zio.client

import java.net.URI
import java.util.Collections
import java.util.concurrent.Executors

import io.deephaven.client.impl.{ClientConfig, ExportId, FlightDescriptorHelper, SessionConfig, SessionFactoryConfig, SessionImpl, SessionMiddleware}
import io.deephaven.proto.DeephavenChannelImpl
import io.deephaven.proto.backplane.grpc.{AuthenticationConstantsRequest, ConfigValue}
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
    allocator: BufferAllocator,
    exports: Ref[Chunk[ExportId]]
) extends DeephavenService {

  override def publish[A: ArrowSchema](
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

        exportId <- ZIO.attempt(session.newExportId())
        descriptor = FlightDescriptorHelper.descriptor(exportId)

        // Lazily initialize the DoPut stream with the first batch.
        // We keep the root and DoPut listener for subsequent batches.
        // NOTE: session.publish returns a future that may not complete until the server-side export exists,
        // so we intentionally do NOT block on it until after the DoPut completes.
        state <- Ref.make[Option[(FlightClient.ClientStreamListener, VectorSchemaRoot, java.util.concurrent.CompletableFuture[_])]](None)

        _ <- stream
          .grouped(batchSize)
          .runForeach { chunk =>
            state.get.flatMap {
              case None =>
                ZIO.attemptBlocking {
                  val root = ArrowBatch.encodeRows(allocator, chunk)
                  val out = flightClient.startPut(descriptor, root, new AsyncPutListener())
                  // Start streaming the first record batch.
                  out.putNext()
                  // Publish the name immediately, but don't block waiting for it.
                  val publishF = session.publish(tableName, exportId)
                  (out, root, publishF)
                }.flatMap { case (out, root, publishF) =>
                  exports.update(_ :+ exportId) *> state.set(Some((out, root, publishF)))
                }

              case Some((out, root, _publishF)) =>
                ZIO.attemptBlocking {
                  root.clear()
                  root.allocateNew()
                  val writer = ArrowSchema[A].writer(root)
                  chunk.zipWithIndex.foreach { case (row, index) => writer.write(index, row) }
                  writer.setValueCount(chunk.size)
                  root.setRowCount(chunk.size)
                  out.putNext()
                }
            }
          }
          .ensuring {
            state.get.flatMap {
              case None => ZIO.unit
              case Some((out, root, publishF)) =>
                ZIO.attemptBlocking {
                  out.completed()
                  out.getResult()
                  // Ensure the publish future is observed (and propagate failures) after the data upload completes.
                  publishF.get()
                  root.close()
                  ()
                }.ignore
            }
          }
      } yield ()
    }

  override def subscribe[A: ArrowSchema](tableName: String): ZStream[Any, Throwable, A] =
    ZStream.fail(new UnsupportedOperationException("Remote subscribe is not implemented yet"))

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
        exports <- Ref.make(Chunk.empty[ExportId])
        service = new RemoteDeephaven(session, flightClient, allocator, exports)
        _ <- ZIO.addFinalizer(service.close())
        _ <- ZIO.addFinalizer(ZIO.attempt(factory.managedChannel().shutdown()).ignore)
      } yield service
    }
}
