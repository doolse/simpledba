package io.doolse.simpledba.test

import cats.Monad
import cats.effect.Sync
import io.doolse.simpledba.{JavaEffects, Streamable}

trait TestEffects[S[_], F[_]] {

  def last[A](s: S[A]): F[Option[A]]
  def toVector[A](s: S[A]): F[Vector[A]]
  def streamable: Streamable[S, F]
  def SM : Monad[S]
  def sync : Sync[F]
  def javaEffects : JavaEffects[F]
}
