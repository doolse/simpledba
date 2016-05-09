package io.doolse.simpledba

import cats.Monad
import cats.syntax.all._
import shapeless.ops.product.ToHList
import shapeless.{::, HList, HNil}

/**
  * Created by doolse on 8/05/16.
  */

trait ValueConvert[V, L] extends (V => L)

object ValueConvert {
  implicit def singleValue[V, L <: HList](implicit ev: (V :: HNil) =:= L) = new ValueConvert[V, L] {
    override def apply(v1: V): L = ev(v1 :: HNil)
  }

  implicit def tupleValue[V, L <: HList](implicit toList: ToHList.Aux[V, L]) = new ValueConvert[V, L] {
    override def apply(v1: V): L = toList(v1)
  }
}

abstract class WriteQueries[F[_] : Monad, T] {
  self =>
  def insert: T ⇒ F[Unit]

  def update: (T, T) => F[Boolean]

  def deleteByValue: T => F[Unit]

  def denormalize[T2](dupe: WriteQueries[F, T2], f: T => T2): WriteQueries[F, T] = new WriteQueries[F, T] {
    def insert = (t: T) => dupe.insert(f(t)).flatMap(_ => self.insert(t))

    def update = (ex: T, up: T) => dupe.update(f(ex), f(up)).flatMap(_ => self.update(ex, up))

    def deleteByValue = (t: T) => dupe.deleteByValue(f(t)).flatMap(_ => self.deleteByValue(t))
  }
}

case class RelationQueries[F[_] : Monad, T, Key, PartKey]
(insert: T ⇒ F[Unit],
 update: (T, T) => F[Boolean],
 queryByKey: Key ⇒ F[Option[T]],
 deleteByPartKey: PartKey => F[Unit],
 delete: Key ⇒ F[Unit],
 deleteByValue: T => F[Unit]) extends WriteQueries[F, T] {

  def keys[NKey, NPartKey](implicit convKey: ValueConvert[NKey, Key], convPart: ValueConvert[NPartKey, PartKey]) = {
    copy[F, T, NKey, NPartKey](queryByKey = convKey andThen queryByKey, delete = convKey andThen delete, deleteByPartKey = convPart andThen deleteByPartKey)
  }

  def key[NKey](implicit convKey: ValueConvert[NKey, Key]) = {
    copy[F, T, NKey, PartKey](queryByKey = convKey andThen queryByKey, delete = convKey andThen delete)
  }
}
