package deephaven.zio.arrow

import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, LocalTime}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, ListVector}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType => ArrowFieldType, Schema}
import org.apache.arrow.vector.util.Text

import shapeless._
import shapeless.labelled._

import scala.jdk.CollectionConverters._

trait FieldWriter[T] {
  def set(index: Int, value: T): Unit
  def setNull(index: Int): Unit
  def setValueCount(count: Int): Unit
}

trait FieldReader[T] {
  def isNull(index: Int): Boolean
  def get(index: Int): T
}

trait ArrowFieldCodec[T] {
  def field(name: String): Field
  def writer(vector: FieldVector): FieldWriter[T]
  def reader(vector: FieldVector): FieldReader[T]
}

object ArrowFieldCodec {
  private val DefaultDecimalPrecision = 38
  private val DefaultDecimalScale = 10

  private def notNullableField(name: String, arrowType: ArrowType): Field =
    new Field(name, new ArrowFieldType(false, arrowType, null, null), List.empty[Field].asJava)

  private def nullableField(name: String, arrowType: ArrowType, children: List[Field] = Nil): Field =
    new Field(name, new ArrowFieldType(true, arrowType, null, null), children.asJava)

  implicit val booleanCodec: ArrowFieldCodec[Boolean] = new ArrowFieldCodec[Boolean] {
    override def field(name: String): Field = notNullableField(name, ArrowType.Bool.INSTANCE)

    override def writer(vector: FieldVector): FieldWriter[Boolean] = new FieldWriter[Boolean] {
      private val v = vector.asInstanceOf[BitVector]
      override def set(index: Int, value: Boolean): Unit = v.setSafe(index, if (value) 1 else 0)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Boolean] = new FieldReader[Boolean] {
      private val v = vector.asInstanceOf[BitVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Boolean = v.get(index) == 1
    }
  }

  implicit val intCodec: ArrowFieldCodec[Int] = new ArrowFieldCodec[Int] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Int(32, true))

    override def writer(vector: FieldVector): FieldWriter[Int] = new FieldWriter[Int] {
      private val v = vector.asInstanceOf[IntVector]
      override def set(index: Int, value: Int): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Int] = new FieldReader[Int] {
      private val v = vector.asInstanceOf[IntVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Int = v.get(index)
    }
  }

  implicit val longCodec: ArrowFieldCodec[Long] = new ArrowFieldCodec[Long] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Int(64, true))

    override def writer(vector: FieldVector): FieldWriter[Long] = new FieldWriter[Long] {
      private val v = vector.asInstanceOf[BigIntVector]
      override def set(index: Int, value: Long): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Long] = new FieldReader[Long] {
      private val v = vector.asInstanceOf[BigIntVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Long = v.get(index)
    }
  }

  implicit val floatCodec: ArrowFieldCodec[Float] = new ArrowFieldCodec[Float] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))

    override def writer(vector: FieldVector): FieldWriter[Float] = new FieldWriter[Float] {
      private val v = vector.asInstanceOf[Float4Vector]
      override def set(index: Int, value: Float): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Float] = new FieldReader[Float] {
      private val v = vector.asInstanceOf[Float4Vector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Float = v.get(index)
    }
  }

  implicit val doubleCodec: ArrowFieldCodec[Double] = new ArrowFieldCodec[Double] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))

    override def writer(vector: FieldVector): FieldWriter[Double] = new FieldWriter[Double] {
      private val v = vector.asInstanceOf[Float8Vector]
      override def set(index: Int, value: Double): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Double] = new FieldReader[Double] {
      private val v = vector.asInstanceOf[Float8Vector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Double = v.get(index)
    }
  }

  implicit val byteCodec: ArrowFieldCodec[Byte] = new ArrowFieldCodec[Byte] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Int(8, true))

    override def writer(vector: FieldVector): FieldWriter[Byte] = new FieldWriter[Byte] {
      private val v = vector.asInstanceOf[TinyIntVector]
      override def set(index: Int, value: Byte): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Byte] = new FieldReader[Byte] {
      private val v = vector.asInstanceOf[TinyIntVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Byte = v.get(index)
    }
  }

  implicit val shortCodec: ArrowFieldCodec[Short] = new ArrowFieldCodec[Short] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Int(16, true))

    override def writer(vector: FieldVector): FieldWriter[Short] = new FieldWriter[Short] {
      private val v = vector.asInstanceOf[SmallIntVector]
      override def set(index: Int, value: Short): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Short] = new FieldReader[Short] {
      private val v = vector.asInstanceOf[SmallIntVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Short = v.get(index)
    }
  }

  implicit val charCodec: ArrowFieldCodec[Char] = new ArrowFieldCodec[Char] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Int(16, false))

    override def writer(vector: FieldVector): FieldWriter[Char] = new FieldWriter[Char] {
      private val v = vector.asInstanceOf[UInt2Vector]
      override def set(index: Int, value: Char): Unit = v.setSafe(index, value.toInt)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Char] = new FieldReader[Char] {
      private val v = vector.asInstanceOf[UInt2Vector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Char = v.get(index).toChar
    }
  }

  implicit val stringCodec: ArrowFieldCodec[String] = new ArrowFieldCodec[String] {
    override def field(name: String): Field = notNullableField(name, ArrowType.Utf8.INSTANCE)

    override def writer(vector: FieldVector): FieldWriter[String] = new FieldWriter[String] {
      private val v = vector.asInstanceOf[VarCharVector]
      override def set(index: Int, value: String): Unit = v.setSafe(index, new Text(value))
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[String] = new FieldReader[String] {
      private val v = vector.asInstanceOf[VarCharVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): String = v.getObject(index).toString
    }
  }

  implicit val binaryCodec: ArrowFieldCodec[Array[Byte]] = new ArrowFieldCodec[Array[Byte]] {
    override def field(name: String): Field = notNullableField(name, ArrowType.Binary.INSTANCE)

    override def writer(vector: FieldVector): FieldWriter[Array[Byte]] = new FieldWriter[Array[Byte]] {
      private val v = vector.asInstanceOf[VarBinaryVector]
      override def set(index: Int, value: Array[Byte]): Unit = v.setSafe(index, value)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Array[Byte]] = new FieldReader[Array[Byte]] {
      private val v = vector.asInstanceOf[VarBinaryVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Array[Byte] = v.get(index)
    }
  }

  implicit val decimalCodec: ArrowFieldCodec[BigDecimal] = new ArrowFieldCodec[BigDecimal] {
    override def field(name: String): Field =
      notNullableField(name, new ArrowType.Decimal(DefaultDecimalPrecision, DefaultDecimalScale, 128))

    override def writer(vector: FieldVector): FieldWriter[BigDecimal] = new FieldWriter[BigDecimal] {
      private val v = vector.asInstanceOf[DecimalVector]
      override def set(index: Int, value: BigDecimal): Unit = {
        val scaled = value.setScale(DefaultDecimalScale, BigDecimal.RoundingMode.HALF_UP)
        v.setSafe(index, scaled.bigDecimal)
      }
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[BigDecimal] = new FieldReader[BigDecimal] {
      private val v = vector.asInstanceOf[DecimalVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): BigDecimal = BigDecimal(v.getObject(index))
    }
  }

  implicit val localDateCodec: ArrowFieldCodec[LocalDate] = new ArrowFieldCodec[LocalDate] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Date(DateUnit.DAY))

    override def writer(vector: FieldVector): FieldWriter[LocalDate] = new FieldWriter[LocalDate] {
      private val v = vector.asInstanceOf[DateDayVector]
      override def set(index: Int, value: LocalDate): Unit = v.setSafe(index, value.toEpochDay.toInt)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[LocalDate] = new FieldReader[LocalDate] {
      private val v = vector.asInstanceOf[DateDayVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): LocalDate = LocalDate.ofEpochDay(v.get(index).toLong)
    }
  }

  implicit val localTimeCodec: ArrowFieldCodec[LocalTime] = new ArrowFieldCodec[LocalTime] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Time(TimeUnit.NANOSECOND, 64))

    override def writer(vector: FieldVector): FieldWriter[LocalTime] = new FieldWriter[LocalTime] {
      private val v = vector.asInstanceOf[TimeNanoVector]
      override def set(index: Int, value: LocalTime): Unit = v.setSafe(index, value.toNanoOfDay)
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[LocalTime] = new FieldReader[LocalTime] {
      private val v = vector.asInstanceOf[TimeNanoVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): LocalTime = LocalTime.ofNanoOfDay(v.get(index))
    }
  }

  implicit val instantCodec: ArrowFieldCodec[Instant] = new ArrowFieldCodec[Instant] {
    override def field(name: String): Field = notNullableField(name, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))

    override def writer(vector: FieldVector): FieldWriter[Instant] = new FieldWriter[Instant] {
      private val v = vector.asInstanceOf[TimeStampNanoTZVector]
      override def set(index: Int, value: Instant): Unit = {
        val nanos = value.getEpochSecond * 1000000000L + value.getNano.toLong
        v.setSafe(index, nanos)
      }
      override def setNull(index: Int): Unit = v.setNull(index)
      override def setValueCount(count: Int): Unit = v.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Instant] = new FieldReader[Instant] {
      private val v = vector.asInstanceOf[TimeStampNanoTZVector]
      override def isNull(index: Int): Boolean = v.isNull(index)
      override def get(index: Int): Instant = {
        val nanos = v.get(index)
        val seconds = nanos / 1000000000L
        val remainder = (nanos % 1000000000L).toInt
        Instant.ofEpochSecond(seconds, remainder.toLong)
      }
    }
  }

  implicit def optionCodec[T](implicit base: ArrowFieldCodec[T]): ArrowFieldCodec[Option[T]] = new ArrowFieldCodec[Option[T]] {
    override def field(name: String): Field = {
      val baseField = base.field(name)
      nullableField(name, baseField.getType, baseField.getChildren.asScala.toList)
    }

    override def writer(vector: FieldVector): FieldWriter[Option[T]] = new FieldWriter[Option[T]] {
      private val underlying = base.writer(vector)
      override def set(index: Int, value: Option[T]): Unit = value match {
        case Some(v) => underlying.set(index, v)
        case None => underlying.setNull(index)
      }
      override def setNull(index: Int): Unit = underlying.setNull(index)
      override def setValueCount(count: Int): Unit = underlying.setValueCount(count)
    }

    override def reader(vector: FieldVector): FieldReader[Option[T]] = new FieldReader[Option[T]] {
      private val underlying = base.reader(vector)
      override def isNull(index: Int): Boolean = underlying.isNull(index)
      override def get(index: Int): Option[T] = if (underlying.isNull(index)) None else Some(underlying.get(index))
    }
  }

  implicit def listCodec[T](implicit element: ArrowFieldCodec[T]): ArrowFieldCodec[List[T]] = new ArrowFieldCodec[List[T]] {
    override def field(name: String): Field = {
      val elementField = element.field("element")
      val listType = ArrowType.List.INSTANCE
      new Field(name, new ArrowFieldType(false, listType, null, null), List(elementField).asJava)
    }

    override def writer(vector: FieldVector): FieldWriter[List[T]] = new FieldWriter[List[T]] {
      private val listVector = vector.asInstanceOf[ListVector]
      private val dataVector = listVector.getDataVector
      private val elementWriter = element.writer(dataVector)
      private var currentOffset = 0

      override def set(index: Int, value: List[T]): Unit = {
        listVector.startNewValue(index)
        value.foreach { item =>
          elementWriter.set(currentOffset, item)
          currentOffset += 1
        }
        listVector.endValue(index, value.size)
      }

      override def setNull(index: Int): Unit = listVector.setNull(index)

      override def setValueCount(count: Int): Unit = {
        listVector.setValueCount(count)
        elementWriter.setValueCount(currentOffset)
      }
    }

    override def reader(vector: FieldVector): FieldReader[List[T]] = new FieldReader[List[T]] {
      private val listVector = vector.asInstanceOf[ListVector]
      private val dataVector = listVector.getDataVector
      private val elementReader = element.reader(dataVector)

      override def isNull(index: Int): Boolean = listVector.isNull(index)

      override def get(index: Int): List[T] = {
        val start = listVector.getOffsetBuffer.getInt(index * BaseRepeatedValueVector.OFFSET_WIDTH)
        val end = listVector.getOffsetBuffer.getInt((index + 1) * BaseRepeatedValueVector.OFFSET_WIDTH)
        (start until end).toList.map(elementReader.get)
      }
    }
  }
}

final case class RowWriter[A](write: (Int, A) => Unit, setValueCount: Int => Unit)
final case class RowReader[A](read: Int => A)

trait ArrowSchema[A] {
  def schema: Schema
  def writer(root: VectorSchemaRoot): RowWriter[A]
  def reader(root: VectorSchemaRoot): RowReader[A]
}

object ArrowSchema {
  def apply[A](implicit schema: ArrowSchema[A]): ArrowSchema[A] = schema

  trait HListCodec[L <: HList] {
    def fields: List[Field]
    def writer(root: VectorSchemaRoot): HListWriter[L]
    def reader(root: VectorSchemaRoot): HListReader[L]
  }

  trait HListWriter[L <: HList] {
    def write(index: Int, value: L): Unit
    def setValueCount(count: Int): Unit
  }

  trait HListReader[L <: HList] {
    def read(index: Int): L
  }

  implicit val hnilCodec: HListCodec[HNil] = new HListCodec[HNil] {
    override def fields: List[Field] = Nil
    override def writer(root: VectorSchemaRoot): HListWriter[HNil] = new HListWriter[HNil] {
      override def write(index: Int, value: HNil): Unit = ()
      override def setValueCount(count: Int): Unit = ()
    }
    override def reader(root: VectorSchemaRoot): HListReader[HNil] = new HListReader[HNil] {
      override def read(index: Int): HNil = HNil
    }
  }

  implicit def hconsCodec[K <: Symbol, H, T <: HList](
      implicit
      witness: Witness.Aux[K],
      headCodec: ArrowFieldCodec[H],
      tailCodec: HListCodec[T]
  ): HListCodec[FieldType[K, H] :: T] = new HListCodec[FieldType[K, H] :: T] {
    private val fieldName = witness.value.name

    override def fields: List[Field] = headCodec.field(fieldName) :: tailCodec.fields

    override def writer(root: VectorSchemaRoot): HListWriter[FieldType[K, H] :: T] = {
      val headVector = root.getVector(fieldName)
      val headWriter = headCodec.writer(headVector)
      val tailWriter = tailCodec.writer(root)

      new HListWriter[FieldType[K, H] :: T] {
        override def write(index: Int, value: FieldType[K, H] :: T): Unit = {
          headWriter.set(index, value.head)
          tailWriter.write(index, value.tail)
        }
        override def setValueCount(count: Int): Unit = {
          headWriter.setValueCount(count)
          tailWriter.setValueCount(count)
        }
      }
    }

    override def reader(root: VectorSchemaRoot): HListReader[FieldType[K, H] :: T] = {
      val headVector = root.getVector(fieldName)
      val headReader = headCodec.reader(headVector)
      val tailReader = tailCodec.reader(root)

      new HListReader[FieldType[K, H] :: T] {
        override def read(index: Int): FieldType[K, H] :: T = {
          val head = headReader.get(index)
          val tail = tailReader.read(index)
          field[K](head) :: tail
        }
      }
    }
  }

  implicit def productSchema[A, Repr <: HList](
      implicit
      gen: LabelledGeneric.Aux[A, Repr],
      codec: HListCodec[Repr]
  ): ArrowSchema[A] = new ArrowSchema[A] {
    override val schema: Schema = new Schema(codec.fields.asJava)

    override def writer(root: VectorSchemaRoot): RowWriter[A] = {
      val hlistWriter = codec.writer(root)
      RowWriter[A](
        write = (index, value) => hlistWriter.write(index, gen.to(value)),
        setValueCount = hlistWriter.setValueCount
      )
    }

    override def reader(root: VectorSchemaRoot): RowReader[A] = {
      val hlistReader = codec.reader(root)
      RowReader[A](index => gen.from(hlistReader.read(index)))
    }
  }
}
