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

  def encodeStream[A: ArrowSchema](
      stream: ZStream[Any, Throwable, A],
      allocator: BufferAllocator,
      batchSize: Int
  ): ZStream[Any, Throwable, VectorSchemaRoot] =
    stream.grouped(batchSize).map(chunk => encodeRows[A](allocator, chunk))

  def decodeStream[A: ArrowSchema](batches: ZStream[Any, Throwable, VectorSchemaRoot]): ZStream[Any, Throwable, A] =
    batches.flatMap { root =>
      val reader = ArrowSchema[A].reader(root)
      ZStream.fromIterable(0 until root.getRowCount).map(reader.read)
    }
}
