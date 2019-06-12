package io.doolse.simpledba.jdbc

import cats.effect.Sync

trait JDBCLogger[F[_]] {
  def logPrepare(sql: String): F[Unit]
  def logBind(sql: String, values: Seq[Any]): F[Unit]
}

class NothingLogger[F[_]](implicit A: Sync[F]) extends JDBCLogger[F] {
  override def logPrepare(sql: String): F[Unit] = A.pure()

  override def logBind(sql: String, values: Seq[Any]): F[Unit] = A.pure()
}

case class ConsoleLogger[F[_]](logPrepares: Boolean = false, logBinds: Boolean = true)(
    implicit S: Sync[F])
    extends JDBCLogger[F] {
  override def logPrepare(sql: String): F[Unit] =
    if (logPrepares) S.delay(println(sql)) else S.delay()

  override def logBind(sql: String, values: Seq[Any]): F[Unit] =
    if (logBinds) S.delay(println(s"$sql - $values")) else S.delay()
}
