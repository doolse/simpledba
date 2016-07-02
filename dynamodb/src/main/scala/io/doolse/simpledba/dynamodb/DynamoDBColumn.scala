package io.doolse.simpledba.dynamodb

import java.util.UUID

import com.amazonaws.services.dynamodbv2.model._

/**
  * Created by jolz on 5/05/16.
  */

case class DynamoDBColumn[T](from: AttributeValue => T, to: T => AttributeValue,
                             attributeType: ScalarAttributeType, diff: (T, T) => AttributeValueUpdate,
                             sortablePart: T => String, range: (T, T))

object DynamoDBColumn {

  val EmptyStringValue = "\u0000"

  def create[T](from: AttributeValue => T, to: T => AttributeValue, sortablePart: T => String, range: (T, T), attr: ScalarAttributeType)
  = DynamoDBColumn(from, to, attr, (oldV: T, newV: T) => new AttributeValueUpdate(to(newV), AttributeAction.PUT), sortablePart, range)

  implicit val boolColumn = create[Boolean](_.getBOOL, b => new AttributeValue().withBOOL(b),
    b => if (b) "1" else "0", (false, true), ScalarAttributeType.S)

  implicit val intColumn = create[Int](_.getN.toInt, l => new AttributeValue().withN(l.toString),
    int2sortableString, (Int.MinValue, Int.MaxValue), ScalarAttributeType.N)

  implicit val longColumn = create[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString),
    long2sortableString, (Long.MinValue, Long.MaxValue), ScalarAttributeType.N)

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

  implicit def optionColumn[A](implicit wrapped: DynamoDBColumn[A]) = create[Option[A]](av => Option(av).map(wrapped.from),
    oA => oA.map(wrapped.to).getOrElse(new AttributeValue().withNULL(true)),
    oA => oA.map(wrapped.sortablePart).getOrElse(""), wrapped.range match {
      case (a, b) => (Some(a), Some(b))
    }, wrapped.attributeType)

  def int2sortableString(i: Int) : String = {
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
