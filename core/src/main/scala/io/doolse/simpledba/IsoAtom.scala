package io.doolse.simpledba

import java.util.UUID

/**
  * Created by jolz on 18/11/16.
  */

case class PartialIsoAtom[A, B](from: A => B, to: B => A) {
  def compose[C](iso2: PartialIsoAtom[C, A]): PartialIsoAtom[C, B] = PartialIsoAtom(from.compose(iso2.from), iso2.to.compose(to))
}

case class IsoAtom[A, B](from: A => B, to: B => A)

case class RangedAtom[A, B](from: A => B, to: B => A, defaultValue: A, range: (A, A))

object RangedAtom {
  val uuidRange = (new UUID(0L, 0L), new UUID(-1L, -1L))
  implicit val uuidAtom : RangedAtom[UUID, String] = RangedAtom[UUID, String](_.toString(), UUID.fromString, uuidRange._1, uuidRange)
}

object PartialIsoAtom {
  implicit def fromIso[A, B](implicit iso: IsoAtom[A, B]) = PartialIsoAtom(iso.from, iso.to)
  implicit def fromRange[A, B](implicit iso: RangedAtom[A, B]) = PartialIsoAtom(iso.from, iso.to)
}

object CustomAtom {
  def iso[A, B](from: A => B, to: B => A) : IsoAtom[A, B] = IsoAtom[A, B](from, to)

  def partial[A, B](from: A => B, to: B => A) : PartialIsoAtom[A, B] = PartialIsoAtom[A, B](from, to)

  def ranged[A, B](from: A => B, to: B => A, default: A, range: (A, A)) : RangedAtom[A, B] = RangedAtom[A, B](from,to,default,range)
}
