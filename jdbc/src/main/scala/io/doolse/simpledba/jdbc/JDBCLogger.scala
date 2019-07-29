package io.doolse.simpledba.jdbc

import io.doolse.simpledba.IOEffects

trait JDBCLogger[F[-_, _], R] {
  def logPrepare(sql: String): F[R, Unit]
  def logBind(sql: String, values: Seq[Any]): F[R, Unit]
}

case class NothingLogger[F[-_, _]](implicit E: IOEffects[F]) extends JDBCLogger[F, Any] {
  override def logPrepare(sql: String): F[Any, Unit] = E.unit

  override def logBind(sql: String, values: Seq[Any]): F[Any, Unit] = E.unit
}

case class PrintLnLogger[F[-_, _]](logPrepares: Boolean = false, logBinds: Boolean = true)(
    implicit S: IOEffects[F])
    extends JDBCLogger[F, Any] {
  override def logPrepare(sql: String): F[Any, Unit] =
    if (logPrepares) S.delay(println(sql)) else S.delay()

  override def logBind(sql: String, values: Seq[Any]): F[Any, Unit] =
    if (logBinds) S.delay(println(s"$sql -- Values: ${values.mkString(",")}")) else S.delay()
}
