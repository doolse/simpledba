package io.doolse.simpledba

import cats.{Applicative, Monad}
import cats.syntax.all._
/**
  * Created by jolz on 21/05/16.
  */
case class SingleQuery[F[_], T, KeyValues](query: KeyValues => F[Option[T]]) {
  def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[F, T, K](query = query compose vc)
}

case class MultiQuery[F[_], T, KeyValues](ascending: Boolean, queryWithOrder: (KeyValues, Boolean) => F[List[T]]) {
  def query(k: KeyValues) = queryWithOrder(k, ascending)

  def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[F, T, K](queryWithOrder = (k, asc) => queryWithOrder(vc(k), asc))
}

trait WriteQueries[F[_], T] {
  self =>
  def delete(t: T): F[Unit]

  def insert(t: T): F[Unit]

  def update(existing: T, newValue: T): F[Boolean]

  def combine(other: WriteQueries[F, T])(implicit A: Applicative[F]): WriteQueries[F, T] = new WriteQueries[F, T] {
    def delete(t: T): F[Unit] = self.delete(t) *> other.delete(t)

    def insert(t: T): F[Unit] = self.insert(t) *> other.insert(t)

    def update(existing: T, newValue: T): F[Boolean] = (self.update(existing, newValue) |@| other.update(existing, newValue)).map((a, b) => a || b)
  }
}
