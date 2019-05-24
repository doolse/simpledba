package io.doolse.simpledba

package object syntax {
  implicit class FlushableOps[S[_], A](fa: S[WriteOp])(implicit F: Flushable[S]) {
    def flush: S[Unit] = F.flush(fa)
  }

  implicit class AutoConvertOps[A](a: A) {
    def as[B](implicit convert: AutoConvert[A, B]): B = convert(a)
  }

}
