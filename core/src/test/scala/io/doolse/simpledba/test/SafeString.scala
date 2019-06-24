package io.doolse.simpledba.test

import io.doolse.simpledba.Iso
import org.scalacheck.Arbitrary

/**
  * Created by jolz on 16/06/16.
  */
object SafeString {
  implicit val safeIso = Iso[SafeString, String](_.s, SafeString.apply)
  implicit val arbSafeString : Arbitrary[SafeString] = Arbitrary {
    for {
      s <- Arbitrary.arbitrary[String]
    } yield SafeString(s)
  }
  implicit val ordSS : Ordering[SafeString] = Ordering.by(_.s)
  def apply(s: String): SafeString = new SafeString(if (s.isEmpty) "-" else s)
}

case class SafeString(s: String)
