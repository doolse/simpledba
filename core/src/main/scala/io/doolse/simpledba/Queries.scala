package io.doolse.simpledba

import cats.Monad

trait WriteOp

trait Flushable[S[_[_], _], F[_]]
{
  def flush: S[F, WriteOp] => S[F, Unit]
}

trait Streamable[S[_[_], _], F[_]] {
  def M: Monad[S[F, ?]]
  def eval[A](fa: F[A]): S[F, A]
  def evalMap[A, B](sa: S[F, A])(f: A => F[B]): S[F, B]
  def empty[A]: S[F, A]
  def emit[A](a: A): S[F, A]
  def emits[A](a: Seq[A]): S[F, A]
  def scan[O, O2](s: S[F, O], z: O2)(f: (O2, O) => O2): S[F, O2]
  def append[A](a: S[F, A], b: S[F, A]): S[F, A]
  def bracket[A](acquire: F[A])(release: A => F[Unit]): S[F, A]
  def toVector[A](s: S[F, A]): F[Vector[A]]
  def drain(s: S[F, _]): F[Unit]
}

trait WriteQueries[S[_[_], _], F[_], T] {
  self =>

  protected def S: Streamable[S, F]

  def insertAll: S[F, T] => S[F, WriteOp]

  def updateAll: S[F, (T, T)] => S[F, WriteOp]

  def insert(t: T): S[F, WriteOp] = insertAll(S.emit(t))

  def update(old: T, next: T): S[F, WriteOp] = updateAll(S.emit((old, next)))

  def replaceAll[K](f: T => K): (S[F, T], S[F, T]) => S[F, WriteOp] =
    (exstream, newstream) => {
      val M = S.M

      M.flatMap {
        S.scan(exstream, Map.empty[K, T])((m, t) => m.updated(f(t), t))
      } { exmap =>
        M.flatMap {
          S.scan(newstream, (S.empty[WriteOp], exmap)) { (m, t) =>
            val k  = f(t)
            val ex = exmap.get(k)
            ex match {
              case None      => m.copy(_1 = S.append(m._1, insert(t)))
              case Some(exT) => (S.append(m._1, update(exT, t)), m._2 - k)
            }
          }
        } { case (w, toDelete) => S.append(w, deleteAll(S.emits(toDelete.values.toSeq))) }
      }
    }

  def insertUpdateOrDelete(existing: Option[T], newvalue: Option[T]): S[F, WriteOp] =
    (existing, newvalue) match {
      case (None, Some(v))      => insert(v)
      case (Some(ex), Some(nv)) => update(ex, nv)
      case (Some(old), None)    => delete(old)
      case _                    => S.empty
    }

  def deleteAll: S[F, T] => S[F, WriteOp]

  def delete(t: T) = deleteAll(S.emit(t))
}
