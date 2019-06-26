package io.doolse.simpledba.dynamodb

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

import shapeless._
import shapeless.HNil
import software.amazon.awssdk.core.SdkBytes

trait BinaryKey[A] { self =>
  def write(a: A, append: DataOutputStream): Unit
  def cmap[B](f: B => A): BinaryKey[B] = (a: B, append: DataOutputStream) => self.write(f(a), append)
}

object BinaryKey
{
  def apply[A](a: A)(implicit keyBuilder: BinaryKey[A]): SdkBytes = {
    val baos = new ByteArrayOutputStream()
    keyBuilder.write(a, new DataOutputStream(baos))
    SdkBytes.fromByteArray(baos.toByteArray)
  }

  implicit def intBinaryKey: BinaryKey[Int] = (a: Int, append: DataOutputStream) => {
    append.writeInt(a - Int.MinValue)
  }

  implicit def shortBinaryKey: BinaryKey[Short] = (a: Short, append: DataOutputStream) => {
    append.writeShort(a - Short.MinValue)
  }

  implicit def boolBinaryKey: BinaryKey[Boolean] = (a: Boolean, append: DataOutputStream) => append.writeBoolean(a)

  implicit def stringBinaryKey: BinaryKey[String] = (a: String, append: DataOutputStream) => {
    append.write(a.getBytes(StandardCharsets.UTF_8))
    append.writeByte(0x00)
  }

  implicit def longBinaryKey: BinaryKey[Long] = (a: Long, append: DataOutputStream) => {
    append.writeLong(a - Long.MinValue)
  }

  implicit def floatBinaryKey: BinaryKey[Float] = (a: Float, append: DataOutputStream) => {
    val bits = java.lang.Float.floatToIntBits(a)
    intBinaryKey.write(bits ^ (bits >> 31) & 0x7fffffff, append)
  }

  implicit def uuidBinaryKey: BinaryKey[UUID] = (a: UUID, append: DataOutputStream) => {
    append.writeLong(a.getMostSignificantBits)
    append.writeLong(a.getLeastSignificantBits)
  }

  implicit def hnilBuilder : BinaryKey[HNil] = (a: HNil, append: DataOutputStream) => {}

  implicit def hconsBuilder[H, T <: HList](implicit head: BinaryKey[H], tail: BinaryKey[T]) : BinaryKey[H :: T] = new BinaryKey[H :: T] {
    override def write(a: H :: T, append: DataOutputStream): Unit = {
      head.write(a.head, append)
      tail.write(a.tail, append)
    }
  }

  implicit def genBuilder[A, Repr <: HList](implicit gen: Generic.Aux[A, Repr], keyGen: BinaryKey[Repr]) : BinaryKey[A] =
    keyGen.cmap(gen.to)
}