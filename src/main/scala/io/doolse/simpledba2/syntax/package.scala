package io.doolse.simpledba2

import fs2.Stream

package object syntax {
  implicit class FlushableOps[F[_], A](fa: Stream[F, WriteOp])(implicit F: Flushable[F]) {
    def flush = F.flush(fa)
  }
}
