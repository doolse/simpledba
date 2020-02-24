package io.doolse.simpledba.jdbc

import cats.Applicative
import cats.effect.Sync

trait JDBCLogger[F[_]] {
  def logPrepare(sql: String): F[Unit]
  def logBind(sql: String, values: Seq[Any]): F[Unit]
}

case class NothingLogger[F[_]]()(implicit E: Applicative[F]) extends JDBCLogger[F] {
  override def logPrepare(sql: String): F[Unit] = E.unit

  override def logBind(sql: String, values: Seq[Any]): F[Unit] = E.unit
}

case class PrintLnLogger[F[_]](logPrepares: Boolean = false, logBinds: Boolean = true)(
    implicit S: Sync[F])
    extends JDBCLogger[F] {
  override def logPrepare(sql: String): F[Unit] =
    if (logPrepares) S.delay(println(sql)) else S.delay()

  override def logBind(sql: String, values: Seq[Any]): F[Unit] =
    if (logBinds) S.delay(println(s"$sql -- Values: ${values.mkString(",")}")) else S.delay()
}
