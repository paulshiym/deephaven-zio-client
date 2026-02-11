package deephaven.zio.client

import org.apache.arrow.memory.RootAllocator

import deephaven.zio.arrow.ArrowSchema
import zio._
import zio.stream._

trait DeephavenService {
  def publish[A: ArrowSchema: DeephavenTableSchema](
      tableName: String,
      stream: ZStream[Any, Throwable, A],
      batchSize: Int,
      mode: UpdateMode = UpdateMode.AppendOnly
  ): ZIO[Any, Throwable, Unit]

  def subscribe[A: ArrowSchema: DeephavenRowDecoder](tableName: String): ZStream[Any, Throwable, A]
}

object DeephavenService {
  def publish[A: ArrowSchema: DeephavenTableSchema](
      tableName: String,
      stream: ZStream[Any, Throwable, A],
      batchSize: Int,
      mode: UpdateMode = UpdateMode.AppendOnly
  ): ZIO[DeephavenService, Throwable, Unit] =
    ZIO.serviceWithZIO[DeephavenService](_.publish(tableName, stream, batchSize, mode))

  def subscribe[A: ArrowSchema: DeephavenRowDecoder](tableName: String): ZStream[DeephavenService, Throwable, A] =
    ZStream.serviceWithStream[DeephavenService](_.subscribe[A](tableName))

  val live: ZLayer[Any, Throwable, DeephavenService] =
    ZLayer.scoped {
      for {
        allocator <- ZIO.acquireRelease(ZIO.attempt(new RootAllocator(Long.MaxValue)))(alloc => ZIO.attempt(alloc.close()).ignore)
        inMemory <- ZIO.acquireRelease(InMemoryDeephaven.make(allocator))(_.close())
      } yield new DeephavenService {
        override def publish[A: ArrowSchema: DeephavenTableSchema](
            tableName: String,
            stream: ZStream[Any, Throwable, A],
            batchSize: Int,
            mode: UpdateMode
        ): ZIO[Any, Throwable, Unit] =
          inMemory.publish(tableName, stream, batchSize, mode)

        override def subscribe[A: ArrowSchema: DeephavenRowDecoder](tableName: String): ZStream[Any, Throwable, A] =
          inMemory.subscribe(tableName)
      }
    }

  def remote(host: String, port: Int, psk: String): ZLayer[Any, Throwable, DeephavenService] =
    RemoteDeephaven.layer(host, port, psk)
}
