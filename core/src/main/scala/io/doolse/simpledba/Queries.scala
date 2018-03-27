package io.doolse.simpledba
import fs2.{Pipe, Sink, Stream}

trait WriteOp

trait Flushable[F[_]] {
  def flush : Sink[F, WriteOp]
}

trait WriteQueries[F[_], T] {

  def insertAll: Pipe[F, T, WriteOp]

  def updateAll: Pipe[F, (T,T), WriteOp]

  def insert(t: T): Stream[F, WriteOp] = insertAll(Stream(t))

  def update(old: T, next: T): Stream[F, WriteOp] = updateAll(Stream((old,next)))

  def deleteAll: Pipe[F, T, WriteOp]

  def delete(t: T) = deleteAll(Stream(t))

  def truncate: Stream[F, WriteOp]
}
