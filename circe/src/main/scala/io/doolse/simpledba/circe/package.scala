package io.doolse.simpledba

import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import io.circe.parser._
package object circe {

  def circeIso[A: Encoder: Decoder](default: A): Iso[A, Option[String]] =
    new Iso[A, Option[String]](
      a => Option(a.asJson.noSpaces), {
        case None    => default
        case Some(s) => decode[A](s).getOrElse(default)
      }
    )

  def circeIsoUnsafe[A: Encoder: Decoder]: Iso[A, String] = new Iso[A, String](
    _.asJson.noSpaces,
    s => decode[A](s).fold(throw _, identity)
  )

  def circeIsoNonNull[A: Encoder: Decoder](default: A): Iso[A, String] = new Iso[A, String](
    _.asJson.noSpaces,
    s => decode[A](s).getOrElse(default)
  )
}
