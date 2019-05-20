package io.doolse.simpledba.test

import org.scalacheck.{Arbitrary, Gen, Properties}

/**
  * Created by jolz on 3/07/16.
  */
abstract class SimpleDBAProperties(name: String) extends Properties(name) {
  implicit def arbUUID = Arbitrary(Gen.uuid)

}
