package deephaven.zio.client

import io.deephaven.engine.table.{ColumnSource, Table}

import shapeless._
import shapeless.labelled._

import java.time.Instant

/** Decode a Deephaven engine Table row into a Scala value.
  *
  * Used for remote live subscriptions (Barrage), where the client holds a local updating Table.
  */
trait DeephavenRowDecoder[A] {
  def decode(table: Table, rowKey: Long): A
}

object DeephavenRowDecoder {
  def apply[A](implicit ev: DeephavenRowDecoder[A]): DeephavenRowDecoder[A] = ev

  trait ColumnDecoder[T] {
    def decode(column: ColumnSource[_], rowKey: Long): T
  }

  object ColumnDecoder {
    private def obj[T](column: ColumnSource[_], rowKey: Long): T =
      column.get(rowKey).asInstanceOf[T]

    implicit val boolean: ColumnDecoder[Boolean] = (c, k) => obj[java.lang.Boolean](c, k).booleanValue()
    implicit val int: ColumnDecoder[Int] = (c, k) => obj[java.lang.Integer](c, k).intValue()
    implicit val long: ColumnDecoder[Long] = (c, k) => obj[java.lang.Long](c, k).longValue()
    implicit val float: ColumnDecoder[Float] = (c, k) => obj[java.lang.Float](c, k).floatValue()
    implicit val double: ColumnDecoder[Double] = (c, k) => obj[java.lang.Double](c, k).doubleValue()
    implicit val string: ColumnDecoder[String] = (c, k) => obj[String](c, k)
    implicit val instant: ColumnDecoder[Instant] = (c, k) => obj[Instant](c, k)

    implicit def option[T](implicit base: ColumnDecoder[T]): ColumnDecoder[Option[T]] =
      (c, k) => {
        val v = c.get(k)
        if (v == null) None else Some(base.decode(c, k))
      }
  }

  trait HListDecoder[L <: HList] {
    def decode(table: Table, rowKey: Long): L
  }

  object HListDecoder {
    implicit val hnil: HListDecoder[HNil] = (_: Table, _: Long) => HNil

    implicit def hcons[K <: Symbol, H, T <: HList](
        implicit
        witness: Witness.Aux[K],
        head: ColumnDecoder[H],
        tail: HListDecoder[T]
    ): HListDecoder[FieldType[K, H] :: T] = new HListDecoder[FieldType[K, H] :: T] {
      private val name = witness.value.name
      override def decode(table: Table, rowKey: Long): FieldType[K, H] :: T = {
        val col = table.getColumnSource(name)
        field[K](head.decode(col, rowKey)) :: tail.decode(table, rowKey)
      }
    }
  }

  implicit def product[A, Repr <: HList](
      implicit
      gen: LabelledGeneric.Aux[A, Repr],
      dec: HListDecoder[Repr]
  ): DeephavenRowDecoder[A] = (table: Table, rowKey: Long) => gen.from(dec.decode(table, rowKey))
}
