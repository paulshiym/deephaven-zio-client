package deephaven.zio.arrow

import java.time.{Instant, LocalDate, LocalTime}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType

import zio._
import zio.test._

import scala.jdk.CollectionConverters._

final case class PrimitiveRow(
    bool: Boolean,
    int: Int,
    long: Long,
    float: Float,
    double: Double,
    string: String,
    byte: Byte,
    short: Short,
    char: Char,
    binary: Array[Byte],
    decimal: BigDecimal,
    date: LocalDate,
    time: LocalTime,
    timestamp: Instant
)

final case class OptionalRow(
    maybeInt: Option[Int],
    maybeString: Option[String],
    maybeBytes: Option[Array[Byte]],
    maybeList: Option[List[String]]
)

final case class ListRow(
    ints: List[Int],
    strings: List[String],
    options: List[Option[Long]]
)

object ArrowSchemaSpec extends ZIOSpecDefault {

  private def scopedAllocator: ZIO[Scope, Nothing, RootAllocator] =
    ZIO.acquireRelease(ZIO.succeed(new RootAllocator(Long.MaxValue)))(a => ZIO.succeed(a.close()).ignore)

  private def scopedRoot(root: VectorSchemaRoot): ZIO[Scope, Nothing, VectorSchemaRoot] =
    ZIO.acquireRelease(ZIO.succeed(root))(r => ZIO.succeed(r.close()).ignore)

  override def spec: Spec[TestEnvironment, Any] =
    suite("ArrowSchemaSpec")(
      test("round-trip for primitive types") {
        ZIO.scoped {
          for {
            allocator <- scopedAllocator
            row = PrimitiveRow(
              bool = true,
              int = 42,
              long = 9000000000L,
              float = 1.25f,
              double = 9.75,
              string = "alpha",
              byte = 7.toByte,
              short = 32000.toShort,
              char = 'Z',
              binary = Array[Byte](1, 2, 3, 4),
              decimal = BigDecimal("1234.5678"),
              date = LocalDate.of(2024, 1, 15),
              time = LocalTime.of(12, 30, 45, 123456789),
              timestamp = Instant.parse("2024-01-15T12:30:45.123456789Z")
            )
            root <- scopedRoot(ArrowBatch.encodeRows(allocator, Chunk(row)))
            decoded = ArrowBatch.decodeRows[PrimitiveRow](root).head
          } yield
            assertTrue(decoded.bool == row.bool) &&
              assertTrue(decoded.int == row.int) &&
              assertTrue(decoded.long == row.long) &&
              assertTrue(decoded.float == row.float) &&
              assertTrue(decoded.double == row.double) &&
              assertTrue(decoded.string == row.string) &&
              assertTrue(decoded.byte == row.byte) &&
              assertTrue(decoded.short == row.short) &&
              assertTrue(decoded.char == row.char) &&
              assertTrue(decoded.binary.sameElements(row.binary)) &&
              assertTrue(decoded.decimal == row.decimal) &&
              assertTrue(decoded.date == row.date) &&
              assertTrue(decoded.time == row.time) &&
              assertTrue(decoded.timestamp == row.timestamp)
        }
      },
      test("round-trip for option and list types") {
        ZIO.scoped {
          for {
            allocator <- scopedAllocator
            row1 = OptionalRow(Some(1), None, Some(Array[Byte](9, 8)), Some(List("a", "b")))
            row2 = OptionalRow(None, Some("beta"), None, None)
            listRow = ListRow(List(1, 2, 3), List("x", "y"), List(Some(10L), None, Some(20L)))

            root <- scopedRoot(ArrowBatch.encodeRows(allocator, Chunk(row1, row2)))
            decoded = ArrowBatch.decodeRows[OptionalRow](root)
            decodedRow1 = decoded(0)
            decodedRow2 = decoded(1)

            root2 <- scopedRoot(ArrowBatch.encodeRows(allocator, Chunk(listRow)))
            decodedList = ArrowBatch.decodeRows[ListRow](root2).head
          } yield
            assertTrue(decodedRow1.maybeInt == row1.maybeInt) &&
              assertTrue(decodedRow1.maybeString == row1.maybeString) &&
              assertTrue(decodedRow1.maybeBytes.exists(_.sameElements(row1.maybeBytes.get))) &&
              assertTrue(decodedRow1.maybeList == row1.maybeList) &&
              assertTrue(decodedRow2.maybeInt == row2.maybeInt) &&
              assertTrue(decodedRow2.maybeString == row2.maybeString) &&
              assertTrue(decodedRow2.maybeBytes == row2.maybeBytes) &&
              assertTrue(decodedRow2.maybeList == row2.maybeList) &&
              assertTrue(decodedList.ints == listRow.ints) &&
              assertTrue(decodedList.strings == listRow.strings) &&
              assertTrue(decodedList.options == listRow.options)
        }
      },
      test("schema correctness for option and list") {
        val schema = ArrowSchema[OptionalRow].schema
        val fields = schema.getFields.asScala
        val names = fields.map(_.getName).toList

        val optionField = fields.find(_.getName == "maybeInt").get
        val listField = fields.find(_.getName == "maybeList").get

        assertTrue(names == List("maybeInt", "maybeString", "maybeBytes", "maybeList")) &&
          assertTrue(optionField.isNullable) &&
          assertTrue(listField.isNullable) &&
          assertTrue(listField.getType.isInstanceOf[ArrowType.List])
      }
    )
}
