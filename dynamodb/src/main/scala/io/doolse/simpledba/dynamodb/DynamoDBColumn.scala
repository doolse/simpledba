package io.doolse.simpledba.dynamodb

import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.{IsoAtom, PartialIsoAtom, RangedAtom}

import scala.collection.JavaConverters._

/**
  * Created by jolz on 5/05/16.
  */

trait DynamoDBColumn[T] {
  val from: Option[AttributeValue] => Option[T]

  def default: T

  val to: T => AttributeValue
  val diff: (T, T) => AttributeValueUpdate
}

trait DynamoDBKeyColumn[T] extends DynamoDBColumn[T] {
  val attributeType: ScalarAttributeType
  val sortablePart: T => String
  val range: (T, T)
}

case class DynamoDBScalar[T](
  from: Option[AttributeValue] => Option[T],
  default: T,
  to: T => AttributeValue,
  diff: (T, T) => AttributeValueUpdate,
  attributeType: ScalarAttributeType,
  sortablePart: T => String,
  range: (T, T)) extends DynamoDBKeyColumn[T]

trait DynamoDBColumnLP2 {
  implicit def isoColumn[A, B](implicit iso: PartialIsoAtom[A, B], oc: DynamoDBColumn[B]): DynamoDBColumn[A] = new DynamoDBColumn[A] {
    val from = oc.from.andThen(_.map(iso.to))
    val to = iso.from andThen oc.to
    val diff = (o: A, n: A) => oc.diff(iso.from(o), iso.from(n))

    def default = iso.to(oc.default)
  }
}

trait DynamoDBColumnLP extends DynamoDBColumnLP2 {

  implicit def rangedIsoColumn[A, B](implicit iso: RangedAtom[A, B], oc: DynamoDBScalar[B]): DynamoDBKeyColumn[A] = new DynamoDBKeyColumn[A] {
    val from = oc.from.andThen(_.map(iso.to))
    val to = iso.from andThen oc.to
    val diff = (o: A, n: A) => oc.diff(iso.from(o), iso.from(n))

    def default = iso.defaultValue

    val attributeType = oc.attributeType
    val range: (A, A) = iso.range
    val sortablePart: A => String = oc.sortablePart compose iso.from
  }

  implicit def isoKeyColumn[A, B](implicit iso: IsoAtom[A, B], oc: DynamoDBScalar[B]): DynamoDBKeyColumn[A] = new DynamoDBKeyColumn[A] {
    val from = oc.from.andThen(_.map(iso.to))
    val to = iso.from andThen oc.to
    val diff = (o: A, n: A) => oc.diff(iso.from(o), iso.from(n))
    val default = iso.to(oc.default)

    val attributeType = oc.attributeType
    val range = {
      val (l, r) = oc.range
      (iso.to(l), iso.to(r))
    }
    val sortablePart = oc.sortablePart compose iso.from

  }

  implicit def fromUuidIso[A](implicit iso: PartialIsoAtom[A, UUID]): PartialIsoAtom[A, String] =
    PartialIsoAtom(RangedAtom.uuidAtom.from.compose(iso.from), RangedAtom.uuidAtom.to.andThen(iso.to))

}

object DynamoDBColumn extends DynamoDBColumnLP {

  val EmptyStringValue = "\u0000"
  val EmptyStringSetValue = "\u0000\u0000"

  def putUpdate[T](to: T => AttributeValue)(oldV: T, newV: T) = new AttributeValueUpdate(to(newV), AttributeAction.PUT)

  def create[T](_default: T, _from: AttributeValue => T, _to: T => AttributeValue, _sortablePart: T => String, _range: (T, T), attr: ScalarAttributeType)
  = DynamoDBScalar[T](_.map(_from), _default, _to, putUpdate(_to), attr, _sortablePart, _range)

  implicit val boolColumn = create[Boolean](false, _.getBOOL, b => new AttributeValue().withBOOL(b),
    b => if (b) "1" else "0", (false, true), ScalarAttributeType.S)

  implicit val intColumn = create[Int](0, _.getN.toInt, l => new AttributeValue().withN(l.toString),
    int2sortableString, (Int.MinValue, Int.MaxValue), ScalarAttributeType.N)

  implicit val longColumn = create[Long](0L, _.getN.toLong, l => new AttributeValue().withN(l.toString),
    long2sortableString, (Long.MinValue, Long.MaxValue), ScalarAttributeType.N)

  implicit val dateColumn = create[Date](new Date(0L), a => new Date(a.getN.toLong), l => new AttributeValue().withN(l.getTime.toString),
    d => long2sortableString(d.getTime), (new Date(Long.MinValue), new Date(Long.MaxValue)), ScalarAttributeType.N)

  implicit val shortColumn = create[Short](0, _.getN.toShort, l => new AttributeValue().withN(l.toString),
    s => int2sortableString(s.toInt), (Short.MinValue, Short.MaxValue), ScalarAttributeType.N)

  implicit val floatColumn = create[Float](0.0f, _.getN.toFloat, l => new AttributeValue().withN(l.toString),
    float2sortableString, (Float.MinValue, Float.MaxValue), ScalarAttributeType.N)

  implicit val doubleColumn = create[Double](0.0, _.getN.toDouble, l => new AttributeValue().withN(l.toString),
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

    create[String]("", decodeBlank, encodeBlank, identity, ("\u0000", "\uffff"), ScalarAttributeType.S)
  }

  private def checkEmptyVec(s: Vector[String]): Vector[String] = if (s.size == 1 && s.head == EmptyStringSetValue) Vector.empty else s

  private def checkEmptySet(s: Set[String]): Set[String] = if (s.size == 1 && s.head == EmptyStringSetValue) Set.empty else s

  private def fixEmpty(s: List[String]) = if (s.isEmpty) List(EmptyStringSetValue) else s

  implicit def isoSet[A](implicit iso: PartialIsoAtom[A, String]) = new DynamoDBColumn[Set[A]] {
    def default = Set.empty
    val diff = putUpdate(to)
    val from = _.map( (v:AttributeValue) => checkEmptySet(v.getSS.asScala.toSet).map(iso.to) )
    val to = m => new AttributeValue().withSS(m.map(iso.from).asJava)
  }

  implicit def doubleIsoSet[A, B](implicit iso1: PartialIsoAtom[A, B], iso2: PartialIsoAtom[B, String]) = new DynamoDBColumn[Set[A]] {
    val iso = iso2.compose(iso1)
    def default = Set.empty
    val diff = putUpdate(to)
    val from = _.map( (v:AttributeValue) => checkEmptySet(v.getSS.asScala.toSet).map(iso.to) )
    val to = m => new AttributeValue().withSS(m.map(iso.from).asJava)
  }

  //
  //  implicit val setString = {
  //    create[Set[String]](v => checkEmptySet(v.getSS().asScala.toSet),
  //      s => new AttributeValue(fixEmpty(s.toList).asJava), _.toString(), (Set.empty, Set.empty), ScalarAttributeType.S)
  //  }
  //
  //  implicit val vecUuid = {
  //    create[Vector[UUID]](v => checkEmptyVec(v.getSS().asScala.toVector).map(UUID.fromString),
  //      s => new AttributeValue(fixEmpty(s.toList.map(_.toString)).asJava), _.toString(), (Vector.empty, Vector.empty), ScalarAttributeType.S)
  //  }
  //

  implicit def isoMapColumn[K, V](implicit iso: PartialIsoAtom[K, String], valColumn: DynamoDBColumn[V]) = new DynamoDBColumn[Map[K, V]] {
    def default = Map.empty
    val diff = putUpdate(to)
    val from = _.map( (v:AttributeValue) => v.getM.asScala.map { case (k,v) => (iso.to(k), valColumn.from(Some(v)).getOrElse(valColumn.default))}.toMap )
    val to = m => new AttributeValue().withM(m.map { case (k,v) => (iso.from(k), valColumn.to(v)) } asJava)
  }

  implicit def vecString[A](implicit elem: DynamoDBColumn[A]) = new DynamoDBColumn[Vector[A]] {
    def default = Vector.empty
    val diff = putUpdate(to)
    val from = _.map( (v:AttributeValue) => v.getL().asScala.map(e => elem.from(Some(e)).getOrElse(elem.default)).toVector )
    val to = (s:Vector[A]) => new AttributeValue().withL(s.map(elem.to).asJava)
  }

  implicit def optionColumn[A](implicit wrapped: DynamoDBColumn[A]) = {

    new DynamoDBColumn[Option[A]] {
      val diff = (oldV: Option[A], newV: Option[A]) => (oldV, newV) match {
        case (Some(ex), Some(v)) => wrapped.diff(ex, v)
        case (ex, v) => putUpdate(to)(ex, v)
      }
      val to = (oA: Option[A]) => oA.map(wrapped.to).getOrElse(new AttributeValue().withNULL(true))

      val from = (oAv: Option[AttributeValue]) => oAv.filter(_.isNULL == null).map(av => wrapped.from(Some(av)))
      val default = None
    }
  }


  def int2sortableString(i: Int): String = {
    //    val u = i - Int.MinValue
    //    f"$u%08x"
    val chrs = new Array[Char](3)
    int2sortableChars(i, chrs, 0)
    new String(chrs, 0, 3)
  }

  def int2sortableChars(i: Int, a: Array[Char], offset: Int) {
    val u = i + Int.MinValue
    a(offset) = (u >>> 24).toChar
    a(offset + 1) = ((u >>> 12) & 0x0fff).toChar
    a(offset + 2) = (u & 0x0fff).toChar
  }

  def long2sortableString(l: Long): String = {
    //    val u = l - Long.MinValue
    //    f"$u%016x"
    val chrs = new Array[Char](5)
    long2sortableChars(l, chrs, 0)
    new String(chrs, 0, 5)
  }

  def long2sortableChars(l: Long, a: Array[Char], offset: Int) {
    val u = l + Long.MinValue
    a(offset) = (u >>> 60).toChar
    a(offset + 1) = (u >>> 45 & 0x7fff).toChar
    a(offset + 2) = (u >>> 30 & 0x7fff).toChar
    a(offset + 3) = (u >>> 15 & 0x7fff).toChar
    a(offset + 4) = (u & 0x7fff).toChar
  }

  def float2sortableString(f: Float) = {
    val i = java.lang.Float.floatToRawIntBits(f)
    val i2 = if (i < 0) i ^ 0x7fffffff else i
    int2sortableString(i2)
  }

  def double2sortableString(d: Double) = {
    val l = java.lang.Double.doubleToRawLongBits(d)
    val ul = if (l < 0) l ^ 0x7fffffffffffffffL else l
    long2sortableString(ul)
  }
}
