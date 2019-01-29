package io.doolse.simpledba.jdbc

case class SampleValue[A](v: A)

object SampleValue
{
  implicit val sv: SampleValue[Long] = SampleValue(0L)
}

case class Sequence[A](name: String)
