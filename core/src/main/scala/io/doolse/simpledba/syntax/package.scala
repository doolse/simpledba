package io.doolse.simpledba

package object syntax {
  implicit class FlushableOps[S[_[_], _], F[_], A](fa: S[F, WriteOp])(implicit F: Flushable[S, F]) {
    def flush: S[F, Unit] = F.flush(fa)
  }

  implicit class AutoConvertOps[A](a: A) {
    def as[B](implicit convert: AutoConvert[A, B]): B = convert(a)
  }

}
