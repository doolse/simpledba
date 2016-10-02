package io.doolse.simpledba.dynamodb

import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._

/**
  * Created by jolz on 5/05/16.
  */

case class DynamoDBColumn[T](from: Option[AttributeValue] => Option[T], to: T => AttributeValue,
                             attributeType: ScalarAttributeType, diff: (T, T) => AttributeValueUpdate,
                             sortablePart: T => String, range: Option[(T, T)])

object DynamoDBColumn {

  val EmptyStringValue = "\u0000"
  val EmptyStringSetValue = "\u0000\u0000"

  def putUpdate[T](to: T => AttributeValue)(oldV: T, newV: T) = new AttributeValueUpdate(to(newV), AttributeAction.PUT)

  def create[T](from: AttributeValue => T, to: T => AttributeValue, sortablePart: T => String, range: (T, T), attr: ScalarAttributeType)
  = DynamoDBColumn(_.map(from), to, attr, putUpdate(to), sortablePart, Some(range))

  implicit val boolColumn = create[Boolean](_.getBOOL, b => new AttributeValue().withBOOL(b),
    b => if (b) "1" else "0", (false, true), ScalarAttributeType.S)

  implicit val intColumn = create[Int](_.getN.toInt, l => new AttributeValue().withN(l.toString),
    int2sortableString, (Int.MinValue, Int.MaxValue), ScalarAttributeType.N)

  implicit val longColumn = create[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString),
    long2sortableString, (Long.MinValue, Long.MaxValue), ScalarAttributeType.N)

  implicit val dateColumn = create[Date](a => new Date(a.getN.toLong), l => new AttributeValue().withN(l.getTime.toString),
    d => long2sortableString(d.getTime), (new Date(Long.MinValue), new Date(Long.MaxValue)), ScalarAttributeType.N)

  implicit val shortColumn = create[Short](_.getN.toShort, l => new AttributeValue().withN(l.toString),
    s => int2sortableString(s.toInt), (Short.MinValue, Short.MaxValue), ScalarAttributeType.N)

  implicit val floatColumn = create[Float](_.getN.toFloat, l => new AttributeValue().withN(l.toString),
    float2sortableString, (Float.MinValue, Float.MaxValue), ScalarAttributeType.N)

  implicit val doubleColumn = create[Double](_.getN.toDouble, l => new AttributeValue().withN(l.toString),
    double2sortableString, (Double.MinValue, Double.MaxValue), ScalarAttributeType.N)

  implicit val stringColumn = {
    def decodeBlank(a: AttributeValue): String = {
      val s = a.getS
      if (s == EmptyStringValue) "" else s
    }
    def encodeBlank(s: String): AttributeValue = {
      val rs = if (s.isEmpty) EmptyStringValue else s
      new AttributeValue(rs)
    }
    create[String](decodeBlank, encodeBlank, identity, ("\u0000", "\uffff"), ScalarAttributeType.S)
  }

  implicit val uuidColumn = create[UUID](v => UUID.fromString(v.getS), u => new AttributeValue(u.toString), _.toString(),
    (new UUID(0L, 0L), new UUID(-1L, -1L)), ScalarAttributeType.S)

  private def checkEmptyVec(s: Vector[String]) : Vector[String] = if (s.size == 1 && s.head == EmptyStringSetValue) Vector.empty else s
  private def checkEmptySet(s: Set[String]) : Set[String] = if (s.size == 1 && s.head == EmptyStringSetValue) Set.empty else s
  private def fixEmpty(s: List[String]) = if (s.isEmpty) List(EmptyStringSetValue) else s

  // TODO doesn't make sense for sets to be key parts..
  implicit val setUuid = {
    create[Set[UUID]](v => checkEmptySet(v.getSS().asScala.toSet).map(UUID.fromString),
    s => new AttributeValue(fixEmpty(s.toList.map(_.toString)).asJava), _.toString(), (Set.empty, Set.empty), ScalarAttributeType.S)
  }

  implicit val setString = {
    create[Set[String]](v => checkEmptySet(v.getSS().asScala.toSet),
      s => new AttributeValue(fixEmpty(s.toList).asJava), _.toString(), (Set.empty, Set.empty), ScalarAttributeType.S)
  }

  implicit val vecUuid = {
    create[Vector[UUID]](v => checkEmptyVec(v.getSS().asScala.toVector).map(UUID.fromString),
      s => new AttributeValue(fixEmpty(s.toList.map(_.toString)).asJava), _.toString(), (Vector.empty, Vector.empty), ScalarAttributeType.S)
  }

  implicit val vecString = {
    create[Vector[String]](v => checkEmptyVec(v.getSS().asScala.toVector),
      s => new AttributeValue(fixEmpty(s.toList).asJava), _.toString(), (Vector.empty, Vector.empty), ScalarAttributeType.S)
  }

  implicit def optionColumn[A](implicit wrapped: DynamoDBColumn[A]) = {

    def to(oA: Option[A]) = oA.map(wrapped.to).getOrElse(new AttributeValue().withNULL(true))

    def updateOption(oldV: Option[A], newV: Option[A]) = (oldV,newV) match {
      case (Some(ex), Some(v)) => wrapped.diff(ex, v)
      case (ex, v) => putUpdate(to)(ex, v)
    }
    DynamoDBColumn[Option[A]](
      av => Some(wrapped.from(av.filter(_.isNULL == null))),
      to,
      wrapped.attributeType,
      updateOption,
      oA => oA.map(wrapped.sortablePart).getOrElse(""), wrapped.range map {
        case (a, b) => (Some(a), Some(b))
      })
  }

  def int2sortableString(i: Int) : String = {
//    val u = i - Int.MinValue
//    f"$u%08x"
    val chrs = new Array[Char](3)
    int2sortableChars(i, chrs, 0)
    new String(chrs, 0, 3)
  }

  def int2sortableChars(i: Int, a: Array[Char], offset: Int) {
    val u = i+Int.MinValue
    a(offset) = (u >>> 24).toChar
    a(offset+1) = ((u >>> 12) & 0x0fff).toChar
    a(offset+2) = (u & 0x0fff).toChar
  }

  def long2sortableString(l: Long) : String = {
//    val u = l - Long.MinValue
//    f"$u%016x"
    val chrs = new Array[Char](5)
    long2sortableChars(l, chrs, 0)
    new String(chrs, 0, 5)
  }

  def long2sortableChars(l: Long, a: Array[Char], offset: Int) {
    val u = l + Long.MinValue
    a(offset) = (u >>>60).toChar
    a(offset+1) = (u >>>45 & 0x7fff).toChar
    a(offset+2) = (u >>>30 & 0x7fff).toChar
    a(offset+3) = (u >>>15 & 0x7fff).toChar
    a(offset+4) = (u & 0x7fff).toChar
  }

  def float2sortableString(f: Float) = {
    val i = java.lang.Float.floatToRawIntBits(f)
    val i2 = if (i<0) i ^ 0x7fffffff else i
    int2sortableString(i2)
  }

  def double2sortableString(d: Double) = {
    val l = java.lang.Double.doubleToRawLongBits(d)
    val ul = if (l<0) l ^ 0x7fffffffffffffffL else l
    long2sortableString(ul)
  }
}
