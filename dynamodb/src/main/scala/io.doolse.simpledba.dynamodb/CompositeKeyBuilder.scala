package io.doolse.simpledba.dynamodb

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

import shapeless._
import shapeless.HNil
import software.amazon.awssdk.core.SdkBytes

trait CompositeKeyBuilder[A] { self =>
  def apply(a: A, append: DataOutputStream): Unit
  def cmap[B](f: B => A): CompositeKeyBuilder[B] = new CompositeKeyBuilder[B] {
    override def apply(a: B, append: DataOutputStream): Unit = self.apply(f(a), append)
  }
}

object CompositeKeyBuilder
{
  def toSdkBytes[A](a: A)(implicit keyBuilder: CompositeKeyBuilder[A]): SdkBytes = {
    val baos = new ByteArrayOutputStream()
    keyBuilder.apply(a, new DataOutputStream(baos))
    SdkBytes.fromByteArray(baos.toByteArray)
  }

  implicit def intComposite: CompositeKeyBuilder[Int] = (a: Int, append: DataOutputStream) => {
    append.writeInt(a + Int.MaxValue + 1)
  }

  implicit def boolComposite: CompositeKeyBuilder[Boolean] = (a: Boolean, append: DataOutputStream) => append.writeBoolean(a)

  implicit def stringComposite: CompositeKeyBuilder[String] = (a: String, append: DataOutputStream) => {
    append.write(a.getBytes(StandardCharsets.UTF_8))
    append.writeByte(0x00);
  }

  implicit def longComposite: CompositeKeyBuilder[Long] = (a: Long, append: DataOutputStream) => {
    append.writeLong(a)
  }

  implicit def uuidComposite: CompositeKeyBuilder[UUID] = (a: UUID, append: DataOutputStream) => {
    append.writeLong(a.getMostSignificantBits)
    append.writeLong(a.getLeastSignificantBits)
  }

  implicit def hnilBuilder : CompositeKeyBuilder[HNil] = (a: HNil, append: DataOutputStream) => {}

  implicit def hconsBuilder[H, T <: HList](implicit head: CompositeKeyBuilder[H], tail: CompositeKeyBuilder[T]) : CompositeKeyBuilder[H :: T] = new CompositeKeyBuilder[H :: T] {
    override def apply(a: H :: T, append: DataOutputStream): Unit = {
      head.apply(a.head, append)
      tail.apply(a.tail, append)
    }
  }

  implicit def genBuilder[A, Repr <: HList](implicit gen: Generic.Aux[A, Repr], keyGen: CompositeKeyBuilder[Repr]) : CompositeKeyBuilder[A] =
    keyGen.cmap(gen.to)
}