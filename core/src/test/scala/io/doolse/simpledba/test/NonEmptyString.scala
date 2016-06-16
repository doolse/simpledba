package io.doolse.simpledba.test

/**
  * Created by jolz on 16/06/16.
  */
object NonEmptyString {
  implicit def str2NonEmpty(s: String) = new NonEmptyString(s)
}

class NonEmptyString(in: String) {
  val s = if (in.isEmpty) "-" else in

  override def equals(a: Any) = a == s

  override def hashCode = s.hashCode

  override def toString = s
}