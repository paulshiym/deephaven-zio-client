package deephaven.zio.client

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

import deephaven.zio.arrow.{ArrowBatch, ArrowSchema}
import zio._
import zio.stream._

sealed trait UpdateMode
object UpdateMode {
  case object AppendOnly extends UpdateMode
  final case class KeyedUpdates(keys: List[String]) extends UpdateMode
}

final class InMemoryDeephaven private (
    store: Ref[Map[String, InMemoryDeephaven.TableState]],
    allocator: BufferAllocator
) {
  def publish[A: ArrowSchema](
      tableName: String,
      stream: ZStream[Any, Throwable, A],
      batchSize: Int,
      mode: UpdateMode = UpdateMode.AppendOnly
  ): ZIO[Any, Throwable, Unit] = {
    val schema = ArrowSchema[A].schema

    val makeState = for {
      history <- Ref.make(Chunk.empty[VectorSchemaRoot])
      hub <- Hub.unbounded[VectorSchemaRoot]
    } yield InMemoryDeephaven.TableState(schema, history, hub)

    for {
      tables <- store.get
      state <- tables.get(tableName) match {
        case Some(existing) => ZIO.succeed(existing)
        case None =>
          makeState.flatMap { created =>
            store.update(_ + (tableName -> created)).as(created)
          }
      }
      _ <- ArrowBatch
        .encodeStream(stream, allocator, batchSize)
        .runForeach { batch =>
          state.history.update(_ :+ batch) *> state.hub.publish(batch).unit
        }
    } yield ()
  }

  def subscribe[A: ArrowSchema](tableName: String): ZStream[Any, Throwable, A] =
    ZStream.unwrap {
      store.get.map { tables =>
        tables.get(tableName) match {
          case None => ZStream.fail(new NoSuchElementException(s"Table not found: $tableName"))
          case Some(state) =>
            val initial = ZStream.fromZIO(state.history.get).flatMap(chunk => ZStream.fromChunk(chunk))
            val updates = ZStream.fromHub(state.hub)
            ArrowBatch.decodeStream[A](initial ++ updates)
        }
      }
    }

  def close(): UIO[Unit] =
    for {
      tables <- store.get
      _ <- ZIO.foreachDiscard(tables.values) { state =>
        for {
          batches <- state.history.get
          _ <- ZIO.foreachDiscard(batches)(batch => ZIO.attempt(batch.close()).ignore)
          _ <- state.hub.shutdown
        } yield ()
      }
    } yield ()
}

object InMemoryDeephaven {
  private final case class TableState(
      schema: Schema,
      history: Ref[Chunk[VectorSchemaRoot]],
      hub: Hub[VectorSchemaRoot]
  )

  def make(allocator: BufferAllocator): UIO[InMemoryDeephaven] =
    Ref.make(Map.empty[String, TableState]).map(new InMemoryDeephaven(_, allocator))
}
