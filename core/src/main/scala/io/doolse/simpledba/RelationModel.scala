package io.doolse.simpledba

import cats.{Applicative, Functor, Monad}
import cats.syntax.all._
import shapeless.ops.hlist.Prepend
import shapeless._
/**
  * Created by jolz on 21/05/16.
  */

class RelationModel[Relations <: HList, QL <: HList, As[_[_]]](val relations: Relations, val queryList: QL) {
  class QueriesPartial[As0[_[_]]] {
    def apply[QP <: Product, QL <: HList](qp: QP)(implicit gen: Generic.Aux[QP, QL]) = new RelationModel[Relations, QL, As0](relations, gen.to(qp))
  }
  def queries[As0[_[_]]] = new QueriesPartial[As0]
}

object RelationModel {
  def apply[RP <: Product, R <: HList](relations: RP)(implicit gen: Generic.Aux[RP, R]) = new RelationModel[R, HNil, Nothing](gen.to(relations), HNil)
}

class Embed[A]
class CustomAtom[S, A](val to: S => A, val from: A => S)
class Relation[Name, A, Keys <: HList] extends SingletonProductArgs {
  def key(w: Witness) = new Relation[Name, A, w.T :: Keys]
  def keysProduct[L <: HList](keys: L)(implicit p: Prepend[L, Keys]) = new Relation[Name, A, p.Out]
}

trait RelationQuery[K]
class QueryUnique[K, Columns <: HList] extends RelationQuery[K]
class QueryMultiple[K, Columns <: HList, SortColumns <: HList] extends RelationQuery[K]
class QueryRange[K, Columns <: HList, RangeColumns <: HList] extends RelationQuery[K]
class RelationWriter[K]

trait RangeValue[+A]

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
case object NoRange extends RangeValue[Nothing]
case class Inclusive[A](a: A) extends RangeValue[A]
case class Exclusive[A](a: A) extends RangeValue[A]

case class UniqueQuery[F[_], T, Key](query: Key => F[Option[T]]) {
  def apply(kv: Key) = query(kv)
  def as[K](implicit vc: ValueConvert[K, Key]) = copy[F, T, K](query = query compose vc)
}

case class SortableQuery[F[_], T, Key](ascending: Option[Boolean], _q: (Key, Option[Boolean]) => F[List[T]]) {
  def apply(k: Key) = _q(k, ascending)

  def queryWithOrder(k: Key, asc: Boolean) = _q(k, Some(asc))

  def as[K](implicit vc: ValueConvert[K, Key]) = copy[F, T, K](_q = (k, asc) => _q(vc(k), asc))
}

case class RangeQuery[F[_], T, Key, Sort](ascending: Option[Boolean], _q: (Key, RangeValue[Sort], RangeValue[Sort], Option[Boolean]) => F[List[T]]) {
  def apply(k: Key, lower: RangeValue[Sort] = NoRange, higher: RangeValue[Sort] = NoRange) = _q(k, lower, higher, ascending)

  def queryWithOrder(k: Key, lower: RangeValue[Sort] = NoRange, higher: RangeValue[Sort] = NoRange, asc: Boolean) = _q(k, lower, higher, Some(asc))

  def as[K, SK](implicit vc: ValueConvert[K, Key], vc2: ValueConvert[SK, Sort])
  = copy[F, T, K, SK](_q = (k, l, h, asc) => _q(vc(k), l.map(vc2), h.map(vc2), asc))
}

trait WriteQueries[F[_], T] {
  self =>
  def delete(t: T): F[Unit]

  def insert(t: T): F[Unit]

  def update(existing: T, newValue: T): F[Boolean]
}

object WriteQueries {
  def combine[F[_], T](self: WriteQueries[F, T], other: WriteQueries[F, T])(implicit A: Applicative[F]) = new WriteQueries[F, T] {
    def delete(t: T): F[Unit] = self.delete(t) *> other.delete(t)

    def insert(t: T): F[Unit] = self.insert(t) *> other.insert(t)

    def update(existing: T, newValue: T): F[Boolean] = (self.update(existing, newValue) |@| other.update(existing, newValue)).map((a, b) => a || b)
  }
}
