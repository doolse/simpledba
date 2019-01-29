package io.doolse.simpledba

import fs2.Stream

package object syntax {
  implicit class FlushableOps[F[_], A](fa: Stream[F, WriteOp])(implicit F: Flushable[F]) {
    def flush = F.flush(fa)
  }

  implicit class AutoConvertOps[A](a: A)
  {
    def as[B](implicit convert: AutoConvert[A, B]): B = convert(a)
  }

}
