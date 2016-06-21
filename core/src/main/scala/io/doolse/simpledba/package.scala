package io.doolse

import shapeless.{::, DepFn0, DepFn1, Generic, HList, HNil, SingletonProductArgs, Witness}

/**
  * Created by jolz on 2/06/16.
  */
package object simpledba {
  def embed[A] = new Embed[A]

  def atom[S, A](to: S => A, from: A => S) = new CustomAtom(to, from)

  def atom[S, A](gen: Generic[S])(implicit ev: gen.Repr <:< (A :: HNil), ev2: (A :: HNil) <:< gen.Repr)
  = new CustomAtom[S, A](s => ev(gen.to(s)).head, a => gen.from(a :: HNil))

  def relation[A](w: Witness) = new Relation[w.T, A, HNil]

  def query(w: Witness) = new QueryBuilder[w.T]

  def queryByPK(w: Witness) = new QueryPK[w.T]

  def writes(w: Witness) = new RelationWriter[w.T]

  class QueryBuilder[K] {
    // SingletonProductArgs didn't work when used outside of the library
    def multipleByColumns = new WitnessList[QueryBuilder[K], QM](this)
  }

  trait QM[L, A] extends DepFn1[A]
  object QM {
    implicit def qm[L <: HList, K] = new QM[L, QueryBuilder[K]] {
      type Out = QueryMultiple[K, L, HNil]

      def apply(t: QueryBuilder[K]) = new QueryMultiple[K, L, HNil]
    }
  }
  trait SB[L, A] extends DepFn1[A]
  object SB {
    implicit def sb[L <: HList, CL <: HList, K] = new SB[L, QueryMultiple[K, CL, HNil]] {
      type Out = QueryMultiple[K, CL, L]

      def apply(t: QueryMultiple[K, CL, HNil]) = new QueryMultiple[K, CL, L]
    }
  }

  implicit class QueryMultipleOps[K, CL <: HList](qm: QueryMultiple[K, CL, HNil]) {
    def sortBy = new WitnessList[QueryMultiple[K, CL, HNil], SB](qm)
  }
}
