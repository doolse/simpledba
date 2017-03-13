package io.doolse.simpledba

import cats.{Applicative, Functor, Monad, Traverse}
import cats.syntax.all._
import cats.instances.vector._
import shapeless.ops.hlist.Prepend
import shapeless._
import fs2.{Stream, concurrent}
import fs2.util.{Async, Catchable}

/**
  * Created by jolz on 21/05/16.
  */

class RelationModel[Relations <: HList, QL <: HList, As[_[_]]](val relations: Relations, val queryList: QL) {
  class QueriesPartial[As0[_[_]]] {
    def apply[QP <: Product, QL <: HList](qp: QP)(implicit gen: Generic.Aux[QP, QL]) = new RelationModel[Relations, QL, As0](relations, gen.to(qp))
    def apply[Q](q: Q) = new RelationModel[Relations, Q :: HNil, As0](relations, q :: HNil)
  }
  def queries[As0[_[_]]] = new QueriesPartial[As0]
}

object RelationModel {
  def apply[RP <: Product, R <: HList](relations: RP)(implicit gen: Generic.Aux[RP, R]) = new RelationModel[R, HNil, Nothing](gen.to(relations), HNil)
  def apply[R](singleRelation: R) = new RelationModel[R :: HNil, HNil, Nothing](singleRelation :: HNil, HNil)
}

class Embed[A]

class Relation[Name, A, Keys <: HList] extends SingletonProductArgs {
  def key(w: Witness) = new Relation[Name, A, w.T :: Keys]
  def keysProduct[L <: HList](keys: L)(implicit p: Prepend[L, Keys]) = new Relation[Name, A, p.Out]
}

sealed trait RelationReference[K]
sealed trait RelationQuery[K] extends RelationReference[K] {
  def nameHint: Option[String]
}
case class QueryPK[K](nameHint: Option[String]) extends RelationQuery[K] {
  def hint(hint: String) = copy[K](nameHint = Some(hint))
}
case class QueryMultiple[K, Columns <: HList, SortColumns <: HList](nameHint: Option[String]) extends RelationQuery[K]
class RelationWriter[K] extends RelationReference[K]

sealed trait RangeValue[+A] {
  def value = fold((), ()).map(_._1)
  def fold[B](inc: B, exc: B): Option[(A, B)]
}
case class FilterRange[A](lower: RangeValue[A], upper: RangeValue[A]) {
  def contains(v: A)(implicit o: Ordering[A]): Boolean = {
    val inLeft = lower match {
      case NoRange => true
      case Inclusive(a) => o.gteq(v, a)
      case Exclusive(a) => o.gt(v, a)
    }
    inLeft && (upper match {
      case NoRange => true
      case Inclusive(a) => o.lteq(v, a)
      case Exclusive(a) => o.lt(v, a)
    })
  }
}

object RangeValue {
  implicit val functor = new Functor[RangeValue] {
    def map[A, B](fa: RangeValue[A])(f: (A) => B): RangeValue[B] = fa match {
      case NoRange => NoRange
      case Inclusive(a) => Inclusive(f(a))
      case Exclusive(a) => Exclusive(f(a))
    }
  }
  implicit def autoInclusive[A](a: A) : Inclusive[A] = Inclusive(a)
}
case object NoRange extends RangeValue[Nothing] { def fold[B](i: B, x: B) = None }
case class Inclusive[A](a: A) extends RangeValue[A] { def fold[B](i: B, x: B) = Some((a, i)) }
case class Exclusive[A](a: A) extends RangeValue[A] { def fold[B](i: B, x: B) = Some((a, x)) }

case class UniqueQuery[F[_], T, Key](query: Stream[F, Key] => Stream[F, T], queryAll: Stream[F, T]) {
  def apply(kv: Key) = query(Stream.apply[F, Key](kv))
  def as[K](implicit vc: ValueConvert[K, Key]) = copy[F, T, K](query = query.compose(_.map(vc)))
}

case class SortableQuery[F[_], T, Key](ascending: Option[Boolean], _q: (Key, Option[Boolean]) => Stream[F, T]) {
  def apply(k: Key) = _q(k, ascending)

  def queryWithOrder(k: Key, asc: Boolean) = _q(k, Some(asc))

  def as[K](implicit vc: ValueConvert[K, Key]) = copy[F, T, K](_q = (k, asc) => _q(vc(k), asc))
}

case class RangeQuery[F[_], T, Key, Sort](ascending: Option[Boolean], _q: (Key, RangeValue[Sort], RangeValue[Sort], Option[Boolean]) => Stream[F, T]) {
  def apply(k: Key, lower: RangeValue[Sort] = NoRange, higher: RangeValue[Sort] = NoRange) = _q(k, lower, higher, ascending)

  def queryWithOrder(k: Key, lower: RangeValue[Sort] = NoRange, higher: RangeValue[Sort] = NoRange, asc: Boolean) = _q(k, lower, higher, Some(asc))

  def as[K, SK](implicit vc: ValueConvert[K, Key], vc2: ValueConvert[SK, Sort])
  = copy[F, T, K, SK](_q = (k, l, h, asc) => _q(vc(k), l.map(vc2), h.map(vc2), asc))
}

trait WriteQueries[F[_], T] {
  self =>
  def delete(t: T): F[Unit] = bulkDelete(Stream(t))

  def insert(t: T): F[Unit] = bulkInsert(Stream(t))

  def update(existing: T, newValue: T): F[Boolean]

  def truncate : F[Unit]

  def bulkInsert(l: Stream[F, T]): F[Unit]

  def bulkDelete(l: Stream[F, T]): F[Unit]

  def insertUpdateOrDelete(o: Option[T], n: Option[T])(implicit M: Monad[F]): F[Boolean] = (o,n) match {
    case (Some(o), Some(n)) => update(o,n)
    case (None, Some(n)) => M.map(insert(n))(_ => true)
    case (Some(o), None) => M.map(delete(o))(_ => true)
    case _ => M.pure(false)
  }

  def bulkDiff(oldVals: Set[T], newVals: Set[T])(implicit M: Monad[F]): F[Boolean] = {
    val deletes = oldVals &~ newVals
    val inserts = newVals &~ oldVals
    if (deletes.isEmpty && inserts.isEmpty) M.pure(false) else
      bulkDelete(Stream.emits(deletes.toVector)) *> bulkInsert(Stream.emits(inserts.toVector)).map(_ => true)
  }
}

object WriteQueries {
  def combine[F[_], T](self: WriteQueries[F, T], other: WriteQueries[F, T])(implicit A: Applicative[F]) = new WriteQueries[F, T] {
    def update(existing: T, newValue: T): F[Boolean] = (self.update(existing, newValue) |@| other.update(existing, newValue)).map((a, b) => a || b)

    override def bulkInsert(l: Stream[F, T]): F[Unit] = self.bulkInsert(l) *> other.bulkInsert(l)

    override def truncate = self.truncate *> other.truncate

    override def bulkDelete(l: Stream[F, T]): F[Unit] = self.bulkDelete(l) *> other.bulkDelete(l)
  }
}
