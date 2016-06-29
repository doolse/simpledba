package io.doolse.simpledba

import cats.Applicative

/**
  * Created by jolz on 29/06/16.
  */
object CatsUtils {

  def whenM[F[_], A](b: Boolean, fa: => F[A])(implicit M: Applicative[F]): F[Unit] = if (b) M.map(fa)(_ => ()) else M.pure()

}
