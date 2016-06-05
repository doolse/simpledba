package io.doolse

import shapeless.{::, Generic, HNil, Witness}

/**
  * Created by jolz on 2/06/16.
  */
package object simpledba {
  def embed[A] = new Embed[A]

  def atom[S, A](to: S => A, from: A => S) = new CustomAtom(to, from)

  def atom[S, A](gen: Generic[S])(implicit ev: gen.Repr <:< (A :: HNil), ev2: (A :: HNil) <:< gen.Repr)
  = new CustomAtom[S, A](s => ev(gen.to(s)).head, a => gen.from(a :: HNil))

  def relation[A] = new Relation[A, HNil]

  def queryFullKey(w: Witness) = new FullKey[w.T]

  def queryPartialKey(w: Witness, k: Witness) = new PartialKey[w.T, k.T :: HNil]

  def queryPartialKeys(w: Witness, k1: Witness, k2: Witness) = new PartialKey[w.T, k1.T :: k2.T :: HNil]

  def queryWrites(w: Witness) = new RelationWriter[w.T]

}
