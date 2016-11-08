package io.doolse.simpledba

import io.circe._
import io.circe.parser.parse

/**
  * Created by jolz on 3/06/16.
  */
package object circe {

  def circeAtom[A](implicit dec: Decoder[A], enc: Encoder[A]) = new CustomAtom[A, String](a => enc(a).spaces2,
    s => parse(s).flatMap(dec.decodeJson).valueOr(throw _), _ => None)

  def circeSetAtom[A](implicit dec: Decoder[A], enc: Encoder[A]) = new CustomAtom[Set[A], Set[String]](aset => aset.map(a => enc(a).spaces2),
    sset => sset.map(s => parse(s).flatMap(dec.decodeJson).valueOr(throw _) ), _ => None)

  def circeObjectAtom = new CustomAtom[JsonObject, String](j => Printer.spaces2.pretty(Json.fromJsonObject(j)),
    s => parse(s).map(j => j.asObject.getOrElse(sys.error("JSON was not object"))).valueOr(throw _), _ => None)
}
