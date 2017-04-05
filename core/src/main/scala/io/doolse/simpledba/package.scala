package io.doolse

import cats.Functor
import fs2.util.{Catchable, ~>}
import fs2.{Pipe, Stream}
import shapeless.{::, DepFn0, DepFn1, Generic, HList, HNil, SingletonProductArgs, Witness}

/**
  * Created by jolz on 2/06/16.
  */
package object simpledba {
  def embed[A] = new Embed[A]

  def relation[A](w: Witness) = new Relation[w.T, A, HNil]

  def query[K](w: Relation[K, _, _]) = new QueryBuilder[K]

  def queryByPK[K](w: Relation[K, _, _]) = QueryPK[K](None)

  def writes[K](w: Relation[K, _, _]) = new RelationWriter[K]

  class QueryBuilder[K] {
    // SingletonProductArgs didn't work when used outside of the library
    def multipleByColumns = new WitnessList[QueryBuilder[K], QM](this)
  }

  trait QM[L, A] extends DepFn1[A]
  object QM {
    implicit def qm[L <: HList, K] = new QM[L, QueryBuilder[K]] {
      type Out = QueryMultiple[K, L, HNil]

      def apply(t: QueryBuilder[K]) = new QueryMultiple[K, L, HNil](None)
    }
  }
  trait SB[L, A] extends DepFn1[A]
  object SB {
    implicit def sb[L <: HList, CL <: HList, K] = new SB[L, QueryMultiple[K, CL, HNil]] {
      type Out = QueryMultiple[K, CL, L]

      def apply(t: QueryMultiple[K, CL, HNil]) = new QueryMultiple[K, CL, L](None)
    }
  }

  implicit class QueryMultipleOps[K, CL <: HList, SL <: HList](qm: QueryMultiple[K, CL, SL]) {
    def sortBy = new WitnessList[QueryMultiple[K, CL, SL], SB](qm)
    def hint(nameHint: String) : QueryMultiple[K, CL, SL] = qm.copy(nameHint = Some(nameHint))
  }

  trait Flushable[F[_]] {
    def flush[A](f: F[A]): F[A]
  }
  implicit class FlushableOps[F[_], A](fa: F[A])(implicit F: Flushable[F]) {
    def flush = F.flush(fa)
  }

  implicit class ExtendedStreamOps[F[_], A](fa: Stream[F, A])(implicit F: Flushable[F], C: Catchable[F]) {
    def flushEach : Stream[F, A] = Stream.eval(fa.uncons.runLast.flush).flatMap {
      case Some(Some((nec, next))) => Stream.chunk(nec) ++ next.flushEach
      case _ => Stream.empty
    }

    def batchedWrites(batchSize: Int) : Stream[F, A] = fa.rechunkN(batchSize).flushEach
  }
}
