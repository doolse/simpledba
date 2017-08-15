package io.doolse.simpledba

import cats.syntax.all._
import cats.{Applicative, Functor, Monad}
import fs2.{Pipe, Stream}
import fs2.util.Catchable
import shapeless._
import shapeless.ops.hlist.Prepend

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

trait UniqueQuery[F[_], T, Key] { self =>
  def queryAll: Stream[F, T]
  def zipWith[A](k: A => Option[Key]): Pipe[F, A, (A, Option[T])]
  def zipFilter[A](k: A => Option[Key]): Pipe[F, A, (A, T)] = zipWith(k).andThen(_.collect {
    case (a, Some(t)) => (a, t)
  })
  def query: Pipe[F, Key, T] = zipWith(Some.apply[Key]).andThen(_.collect {
    case (_, Some(t)) => t
  })
  def apply(kv: Key) = query(Stream.apply[F, Key](kv))
  def as[K](implicit vc: ValueConvert[K, Key]) : UniqueQuery[F, T, K] = new UniqueQuery[F, T, K] {
    def zipWith[A](l: A => Option[K]) = self.zipWith(a => l(a).map(vc))
    def queryAll = self.queryAll
  }
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

trait Flushable[F[_]] {
  def flush[A](f: Stream[F, WriteOp]): F[Unit]
}

trait WriteOp

trait WriteQueries[F[_], T] {
  self =>

  protected val C: Catchable[F]
  protected val M: Monad[F]
  protected val F: Flushable[F]

  def deleteOp(t: T): Stream[F, WriteOp]

  def insertOp(t: T): Stream[F, WriteOp]

  def updateOp(e: T, n: T): (Boolean, Stream[F, WriteOp])

  def delete(t: T): F[Unit] = F.flush(deleteOp(t))

  def insert(t: T): F[Unit] = F.flush(insertOp(t))

  def truncate: F[Unit]

  def update(existing: T, newValue: T): F[Boolean] = {
    val (u, s) = updateOp(existing, newValue)
    M.map(F.flush(s))(_ => u)
  }

  def bulkInsert(l: Stream[F, T]): F[Unit] = F.flush(l.flatMap(insertOp))

  def bulkDelete(l: Stream[F, T]): F[Unit] = F.flush(l.flatMap(deleteOp))

  def insertUpdateOrDelete(o: Option[T], n: Option[T]): F[Boolean] = (o,n) match {
    case (Some(o), Some(n)) => update(o,n)
    case (None, Some(n)) => M.map(insert(n))(_ => true)
    case (Some(o), None) => M.map(delete(o))(_ => true)
    case _ => M.pure(false)
  }

  def bulkDiff(oldVals: Set[T], newVals: Set[T]): F[Boolean] = {
    implicit val c = M
    val deletes = oldVals &~ newVals
    val inserts = newVals &~ oldVals
    if (deletes.isEmpty && inserts.isEmpty) M.pure(false) else
      bulkDelete(Stream.emits(deletes.toVector)) *> bulkInsert(Stream.emits(inserts.toVector)).map(_ => true)
  }
}

object WriteQueries {
  def combine[F[_], T](self: WriteQueries[F, T], other: WriteQueries[F, T]) = new WriteQueries[F, T] {
    implicit val A = self.M
    val C = self.C
    val M = self.M
    val F = self.F

    def updateOp(existing: T, newValue: T) = {
      val (u1, s1) = self.updateOp(existing, newValue)
      val (u2, s2) = other.updateOp(existing, newValue)
      (u1 || u2, s1 ++ s2)
    }

    override def truncate = self.truncate >> other.truncate

    def insertOp(t: T) = self.insertOp(t) ++ other.insertOp(t)
    def deleteOp(t: T) = self.deleteOp(t) ++ other.deleteOp(t)
  }
}
