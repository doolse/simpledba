package io.doolse.simpledba.test

import org.scalacheck.{Arbitrary, Gen, Properties}

/**
  * Created by jolz on 3/07/16.
  */
abstract class SimpleDBAProperties(name: String) extends Properties(name) {
  def uniqueify[A](rows: Seq[A], f: A => Any): Seq[A] =
    rows.map(t => f(t) -> t).toMap.values.toSeq


}
