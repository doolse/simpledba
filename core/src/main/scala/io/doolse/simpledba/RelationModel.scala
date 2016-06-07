package io.doolse.simpledba

import cats.{Applicative, Monad}
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
class RelationWriter[K] extends RelationQuery[K]

case class SingleQuery[F[_], T, KeyValues](query: KeyValues => F[Option[T]]) {
  def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[F, T, K](query = query compose vc)
}

case class MultiQuery[F[_], T, KeyValues](ascending: Option[Boolean], _q: (KeyValues, Option[Boolean]) => F[List[T]]) {
  def query(k: KeyValues) = _q(k, ascending)

  def queryWithOrder(k: KeyValues, asc: Boolean) = _q(k, Some(asc))

  def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[F, T, K](_q = (k, asc) => _q(vc(k), asc))
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
