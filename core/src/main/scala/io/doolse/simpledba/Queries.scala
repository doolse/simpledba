package io.doolse.simpledba
import fs2.{Pipe, Pipe2, Sink, Stream}

trait WriteOp

trait Flushable[F[_]] {
  def flush: Pipe[F, WriteOp, Unit]
}

trait WriteQueries[F[_], T] { self =>

  def insertAll: Pipe[F, T, WriteOp]

  def updateAll: Pipe[F, (T, T), WriteOp]

  def insert(t: T): Stream[F, WriteOp] = insertAll(Stream(t))

  def update(old: T, next: T): Stream[F, WriteOp] = updateAll(Stream((old, next)))

  def replaceAll[K](f: T => K): Pipe2[F, T, T, WriteOp] =
    (exstream, newstream) =>
      for {
        exmap <- exstream.scan(Map.empty[K, T])((m, t) => m.updated(f(t), t))
        res <- newstream.scan((Stream.empty: Stream[F, WriteOp], exmap)) { (m, t) =>
          val k  = f(t)
          val ex = exmap.get(k)
          ex match {
            case None      => m.copy(_1 = m._1 ++ insert(t))
            case Some(exT) => (m._1 ++ update(exT, t), m._2 - k)
          }
        }
        r <- res._1 ++ deleteAll(Stream.emits(res._2.values.toSeq))
      } yield r

  def insertUpdateOrDelete(existing: Option[T], newvalue: Option[T]): Stream[F, WriteOp] =
    (existing, newvalue) match {
      case (None, Some(v))      => insert(v)
      case (Some(ex), Some(nv)) => update(ex, nv)
      case (Some(old), None)    => delete(old)
      case _                    => Stream.empty
    }

  def deleteAll: Pipe[F, T, WriteOp]

  def delete(t: T) = deleteAll(Stream(t))
}
