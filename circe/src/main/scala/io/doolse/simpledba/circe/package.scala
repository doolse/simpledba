package io.doolse.simpledba

import io.circe.{Decoder, Encoder}
import io.circe.parser.parse

/**
  * Created by jolz on 3/06/16.
  */
package object circe {

  def circeAtom[A](implicit dec: Decoder[A], enc: Encoder[A]) = new CustomAtom[A, String](a => enc(a).spaces2,
    s => parse(s).flatMap(dec.decodeJson).valueOr(throw _) )

  def circeSetAtom[A](implicit dec: Decoder[A], enc: Encoder[A]) = new CustomAtom[Set[A], Set[String]](aset => aset.map(a => enc(a).spaces2),
    sset => sset.map(s => parse(s).flatMap(dec.decodeJson).valueOr(throw _) ))
}
