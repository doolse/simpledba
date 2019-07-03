package io.doolse.simpledba

package object syntax {
  implicit class AutoConvertOps[A](a: A) {
    def as[B](implicit convert: AutoConvert[A, B]): B = convert(a)
  }

}
