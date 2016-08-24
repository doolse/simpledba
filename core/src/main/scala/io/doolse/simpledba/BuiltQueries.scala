package io.doolse.simpledba

import cats.Eval
import shapeless.Poly1
import shapeless.tag.@@

/**
  * Created by jolz on 8/06/16.
  */

trait BuiltQueries[Q] {
  type DDL

  def queries: Q

  def ddl: Iterable[DDL]
}

object BuiltQueries {
  type Aux[Q, DDL0] = BuiltQueries[Q] {type DDL = DDL0}

  def apply[Q, DDL0](q: Q, _ddl: Eval[Iterable[DDL0]]) : Aux[Q, DDL0] = new BuiltQueries[Q] {
    type DDL = DDL0

    def queries = q

    def ddl = _ddl.value
  }
}

trait queriesAsLP extends Poly1 {
  implicit def sq[F[_], FA, FB, T, A, B]
  (implicit ev: FA <:< UniqueQuery[F, T, A], ev2: FB <:< UniqueQuery[F, T, B],
   conv: ValueConvert[B, A]) = at[FA @@ FB] {
    ev(_).as[B]
  }

  implicit def rq[F[_], FA, FB, T, SA, SB, A, B]
  (implicit ev: FA <:< RangeQuery[F, T, A, SA], ev2: FB <:< RangeQuery[F, T, B, SB],
   cv: ValueConvert[B, A], cv2: ValueConvert[SB, SA]) = at[FA @@ FB] {
    ev(_).as[B, SB]
  }

  implicit def mq[F[_], FA, FB, T, A, B, SA]
  (implicit ev: FA <:< RangeQuery[F, T, A, SA], ev2: FB <:< SortableQuery[F, T, B],
   conv: ValueConvert[B, A]) = at[FA @@ FB] { fa =>
    val rq = ev(fa)
    SortableQuery[F, T, B](None, (b, ascO) => rq._q(conv(b), NoRange, NoRange, ascO))
  }
}

object queriesAs extends queriesAsLP {
  implicit def same[A, B](implicit ev: A <:< B) = at[A @@ B](a => a: B)
}
