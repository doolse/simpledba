package io.doolse

import shapeless.{::, Generic, HList, HNil, SingletonProductArgs, Witness}

/**
  * Created by jolz on 2/06/16.
  */
package object simpledba {
  def embed[A] = new Embed[A]

  def atom[S, A](to: S => A, from: A => S) = new CustomAtom(to, from)

  def atom[S, A](gen: Generic[S])(implicit ev: gen.Repr <:< (A :: HNil), ev2: (A :: HNil) <:< gen.Repr)
  = new CustomAtom[S, A](s => ev(gen.to(s)).head, a => gen.from(a :: HNil))

  def relation[A](w: Witness) = new Relation[w.T, A, HNil]

  def query(w: Witness) = new QueryBuilder[w.T, HNil]

  def queryByPK(w: Witness) = new QueryUnique[w.T, HNil]

  def writes(w: Witness) = new RelationWriter[w.T]

  class QueryBuilder[K, SC <: HList] extends SingletonProductArgs {
    def uniqueByColumns(c: Witness) = new QueryUnique[K, c.T :: HNil]
    def uniqueByColumnsProduct[L <: HList](c: L) = new QueryUnique[K, L]
    def multipleByColumns(c: Witness) = new QueryMultiple[K, c.T :: HNil, HNil]
    def multipleByColumnsProduct[L <: HList](c: L) = new QueryMultiple[K, L, HNil]
  }
}
