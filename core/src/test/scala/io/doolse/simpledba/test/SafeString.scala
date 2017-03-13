package io.doolse.simpledba.test

import io.doolse.simpledba.{CustomAtom, IsoAtom}
import org.scalacheck.Arbitrary

/**
  * Created by jolz on 16/06/16.
  */
object SafeString {
  implicit val atom = CustomAtom.iso[SafeString, String](_.s, SafeString.apply)
  implicit val arbSafeString : Arbitrary[SafeString] = Arbitrary {
    for {
      s <- Arbitrary.arbitrary[String]
    } yield {
      val noNul = s.filterNot(_ == 0)
      SafeString(if (noNul.nonEmpty) noNul else "-")
    }
  }
  implicit val ordSS : Ordering[SafeString] = Ordering.by(_.s)
}

case class SafeString(s: String) {
  override def toString = s"($s)-${s.length}"
}
