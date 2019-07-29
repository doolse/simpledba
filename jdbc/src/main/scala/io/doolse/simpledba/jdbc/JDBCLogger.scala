package io.doolse.simpledba.jdbc

import cats.effect.Sync
import io.doolse.simpledba.Streamable

trait JDBCLogger[F[-_, _], R] {
  def logPrepare(sql: String): F[R, Unit]
  def logBind(sql: String, values: Seq[Any]): F[R, Unit]
}

case class NothingLogger[S[-_, _], F[-_, _]](implicit S: Streamable[S, F]) extends JDBCLogger[F, Any] {
  override def logPrepare(sql: String): F[Any, Unit] = S.unit

  override def logBind(sql: String, values: Seq[Any]): F[Any, Unit] = S.unit
}

case class PrintLnLogger[S[-_, _], F[-_, _]](logPrepares: Boolean = false, logBinds: Boolean = true)(
    implicit S: Streamable[S, F])
    extends JDBCLogger[F, Any] {
  override def logPrepare(sql: String): F[Any, Unit] =
    if (logPrepares) S.delay(println(sql)) else S.delay()

  override def logBind(sql: String, values: Seq[Any]): F[Any, Unit] =
    if (logBinds) S.delay(println(s"$sql -- Values: ${values.mkString(",")}")) else S.delay()
}
