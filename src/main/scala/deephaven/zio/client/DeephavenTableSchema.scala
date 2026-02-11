package deephaven.zio.client

import io.deephaven.qst.column.header.ColumnHeader
import io.deephaven.qst.table.TableHeader

import shapeless._
import shapeless.labelled._

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

/** Derive a Deephaven TableHeader (schema) from a Scala product type.
  *
  * This is used for creating Deephaven InputTables for live streaming ingestion.
  */
trait DeephavenTableSchema[A] {
  def header: TableHeader
}

object DeephavenTableSchema {
  def apply[A](implicit ev: DeephavenTableSchema[A]): DeephavenTableSchema[A] = ev

  trait FieldHeader[T] {
    def header(name: String): ColumnHeader[_]
  }

  object FieldHeader {
    private def klassHeader[T](name: String, clazz: Class[T]): ColumnHeader[T] =
      ColumnHeader.of(name, clazz)

    implicit val booleanHeader: FieldHeader[Boolean] = (name: String) => klassHeader(name, classOf[java.lang.Boolean])
    implicit val byteHeader: FieldHeader[Byte] = (name: String) => klassHeader(name, classOf[java.lang.Byte])
    implicit val shortHeader: FieldHeader[Short] = (name: String) => klassHeader(name, classOf[java.lang.Short])
    implicit val intHeader: FieldHeader[Int] = (name: String) => klassHeader(name, classOf[java.lang.Integer])
    implicit val longHeader: FieldHeader[Long] = (name: String) => klassHeader(name, classOf[java.lang.Long])
    implicit val floatHeader: FieldHeader[Float] = (name: String) => klassHeader(name, classOf[java.lang.Float])
    implicit val doubleHeader: FieldHeader[Double] = (name: String) => klassHeader(name, classOf[java.lang.Double])
    implicit val charHeader: FieldHeader[Char] = (name: String) => klassHeader(name, classOf[java.lang.Character])
    implicit val stringHeader: FieldHeader[String] = (name: String) => klassHeader(name, classOf[java.lang.String])

    // Common Deephaven time types
    implicit val instantHeader: FieldHeader[Instant] = (name: String) => klassHeader(name, classOf[Instant])

    // These may depend on server support / type adapters. We still attempt to declare them directly.
    implicit val localDateHeader: FieldHeader[LocalDate] = (name: String) => klassHeader(name, classOf[LocalDate])
    implicit val localTimeHeader: FieldHeader[LocalTime] = (name: String) => klassHeader(name, classOf[LocalTime])
    implicit val localDateTimeHeader: FieldHeader[LocalDateTime] = (name: String) => klassHeader(name, classOf[LocalDateTime])

    implicit def optionHeader[T](implicit base: FieldHeader[T]): FieldHeader[Option[T]] =
      (name: String) => base.header(name)
  }

  trait HListHeaders[L <: HList] {
    def headers: List[ColumnHeader[_]]
  }

  object HListHeaders {
    implicit val hnil: HListHeaders[HNil] = new HListHeaders[HNil] {
      override def headers: List[ColumnHeader[_]] = Nil
    }

    implicit def hcons[K <: Symbol, H, T <: HList](
        implicit
        witness: Witness.Aux[K],
        head: FieldHeader[H],
        tail: HListHeaders[T]
    ): HListHeaders[FieldType[K, H] :: T] = new HListHeaders[FieldType[K, H] :: T] {
      override def headers: List[ColumnHeader[_]] =
        head.header(witness.value.name) :: tail.headers
    }
  }

  implicit def product[A, Repr <: HList](
      implicit
      gen: LabelledGeneric.Aux[A, Repr],
      headers: HListHeaders[Repr]
  ): DeephavenTableSchema[A] = new DeephavenTableSchema[A] {
    override val header: TableHeader = TableHeader.of(headers.headers: _*)
  }
}
