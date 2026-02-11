package deephaven.zio.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import zio._
import zio.stream._

object ArrowBatch {
  def encodeRows[A: ArrowSchema](allocator: BufferAllocator, rows: Chunk[A]): VectorSchemaRoot = {
    val schema = ArrowSchema[A].schema
    val root = VectorSchemaRoot.create(schema, allocator)
    root.allocateNew()
    val writer = ArrowSchema[A].writer(root)
    rows.zipWithIndex.foreach { case (row, index) => writer.write(index, row) }
    writer.setValueCount(rows.size)
    root.setRowCount(rows.size)
    root
  }

  def decodeRows[A: ArrowSchema](root: VectorSchemaRoot): Chunk[A] = {
    val reader = ArrowSchema[A].reader(root)
    val rowCount = root.getRowCount
    Chunk.fromIterable(0 until rowCount).map(reader.read)
  }

  /**
    * Encodes a stream into Arrow batches.
    *
    * IMPORTANT: The returned VectorSchemaRoot values must be closed by the caller.
    * If you want automatic closing, use [[encodeStreamManaged]].
    */
  def encodeStream[A: ArrowSchema](
      stream: ZStream[Any, Throwable, A],
      allocator: BufferAllocator,
      batchSize: Int
  ): ZStream[Any, Throwable, VectorSchemaRoot] =
    stream.grouped(batchSize).map(chunk => encodeRows[A](allocator, chunk))

  /**
    * Encodes a stream into Arrow batches and automatically closes each batch after it is emitted downstream.
    *
    * This is safer for long-running streams to avoid leaking off-heap Arrow memory.
    */
  def encodeStreamManaged[A: ArrowSchema](
      stream: ZStream[Any, Throwable, A],
      allocator: BufferAllocator,
      batchSize: Int
  ): ZStream[Any, Throwable, VectorSchemaRoot] =
    stream.grouped(batchSize).flatMap { chunk =>
      ZStream.acquireReleaseWith(ZIO.attempt(encodeRows[A](allocator, chunk))) {
        (root: VectorSchemaRoot) => ZIO.succeed(root.close()).ignore
      } { (root: VectorSchemaRoot) =>
        ZStream.succeed(root)
      }
    }

  def decodeStream[A: ArrowSchema](batches: ZStream[Any, Throwable, VectorSchemaRoot]): ZStream[Any, Throwable, A] =
    batches.flatMap { root =>
      val reader = ArrowSchema[A].reader(root)
      ZStream.fromIterable(0 until root.getRowCount).map(reader.read)
    }
}
