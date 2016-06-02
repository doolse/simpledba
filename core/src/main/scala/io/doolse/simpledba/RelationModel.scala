package io.doolse.simpledba

import cats.{Applicative, Monad}
import cats.syntax.all._
import shapeless.ops.hlist.Prepend
import shapeless._
/**
  * Created by jolz on 21/05/16.
  */

case class RelationModel[Embedded <: HList, Relations <: HList, Queries <: HList](embedList: Embedded, relationRecord: Relations, queryList: Queries)

object RelationModel {
  def apply[R <: HList, Q <: HList](relations: R, queries: Q) = new RelationModel[HNil, R, Q](HNil : HNil, relations, queries)
}

class Embed[A]
class Atom[S, A](to: S => A, from: A => S)
class Relation[A, Keys <: HList]

class FullKey[K]
class PartialKey[K, Keys <: HList]
class RelationWriter[K]

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
