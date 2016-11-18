package io.doolse.simpledba.test

import io.doolse.simpledba.{CustomAtom, IsoAtom}

/**
  * Created by jolz on 16/06/16.
  */
object NonEmptyString {
  implicit def str2NonEmpty(s: String) = new NonEmptyString(s)

  implicit val atom = CustomAtom.iso[NonEmptyString, String](_.s, new NonEmptyString(_))
}

class NonEmptyString(in: String) {
  val s = if (in.isEmpty) "-" else in

  override def equals(a: Any) = a == s

  override def hashCode = s.hashCode

  override def toString = s
}