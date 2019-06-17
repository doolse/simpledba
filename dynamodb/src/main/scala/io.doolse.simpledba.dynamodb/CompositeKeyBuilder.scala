package io.doolse.simpledba.dynamodb

import java.io.{ByteArrayOutputStream, DataOutputStream}

import shapeless._
import shapeless.HNil

trait CompositeKeyBuilder[A] {
  def apply(a: A, append: DataOutputStream): Unit
}

object CompositeKeyBuilder
{
  implicit def intComposite: CompositeKeyBuilder[Int] = (a: Int, append: DataOutputStream) => {
    append.writeInt(a)
  }

  implicit def boolComposite: CompositeKeyBuilder[Boolean] = (a: Boolean, append: DataOutputStream) => append.writeBoolean(a)

  implicit def stringComposite: CompositeKeyBuilder[String] = (a: String, append: DataOutputStream) => {
    append.writeUTF(a)
  }

  implicit def longComposite: CompositeKeyBuilder[Long] = (a: Long, append: DataOutputStream) => {
    append.writeLong(a)
  }

  implicit def hnilBuilder : CompositeKeyBuilder[HNil] = (a: HNil, append: DataOutputStream) => {}

  implicit def hconsBuilder[H, T <: HList](implicit head: CompositeKeyBuilder[H], tail: CompositeKeyBuilder[T]) : CompositeKeyBuilder[H :: T] = new CompositeKeyBuilder[H :: T] {
    override def apply(a: H :: T, append: DataOutputStream): Unit = {
      head.apply(a.head, append)
      tail.apply(a.tail, append)
    }
  }
}