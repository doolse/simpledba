package io.doolse.simpledba

import java.util.UUID

import cats.arrow.Compose
import cats.instances.function._
import cats.syntax.compose._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Length, LiftAll, Prepend, Split, ToList, ZipWithKeys}
import shapeless.ops.record.{Keys, SelectAll, Selector}
import shapeless.{::, HList, HNil, Nat, Witness}

import scala.annotation.tailrec
import scala.collection.mutable

case class Iso[A, B](to: A => B, from: B => A)

object Iso {
  def id[A]: Iso[A, A] = Iso(identity, identity)

  def uuidString: Iso[UUID, String] = Iso(_.toString, UUID.fromString)

  implicit val composeIso: Compose[Iso] = new Compose[Iso] {
    override def compose[A, B, C](f: Iso[B, C], g: Iso[A, B]): Iso[A, C] =
      Iso(f.to <<< g.to, f.from >>> g.from)
  }
}

class Cols[Keys <: HList] {
  def ++[Keys2 <: HList](cols2: Cols[Keys2])(
      implicit prepend: Prepend[Keys, Keys2]
  ): Cols[prepend.Out] = new Cols[prepend.Out]
}

object Cols {
  def apply(w1: Witness): Cols[w1.T :: HNil]                                           = new Cols
  def apply(w1: Witness, w2: Witness): Cols[w1.T :: w2.T :: HNil]                      = new Cols
  def apply(w1: Witness, w2: Witness, w3: Witness): Cols[w1.T :: w2.T :: w3.T :: HNil] = new Cols
  def apply(
      w1: Witness,
      w2: Witness,
      w3: Witness,
      w4: Witness
  ): Cols[w1.T :: w2.T :: w3.T :: w4.T :: HNil] = new Cols
  def apply(
      w1: Witness,
      w2: Witness,
      w3: Witness,
      w4: Witness,
      w5: Witness
  ): Cols[w1.T :: w2.T :: w3.T :: w4.T :: w5.T :: HNil] = new Cols
}

trait ColumnMapper[C[_], A, B] {
  def apply[V](column: C[V], value: V, a: A): B
}

trait ColumnCompare[C[_], A, B] {
  def apply[V](column: C[V], value1: V, value2: V, a: A): Option[B]
}

trait ColumnRetrieve[C[_], A] {
  def apply[V](column: C[V], offset: Int, a: A): V
}

sealed trait ColumnRecord[C[_], A, R <: HList] {
  def columns: Seq[(A, C[_])]

  def mapRecord[C2[_], A2, B](r: R, f: ColumnMapper[C2, A2, B])(implicit ev: C[_] <:< C2[_], ev2: A <:< A2): Seq[B] = {
    val out = mutable.Buffer[B]()

    @tailrec
    def loop(i: Int, rec: HList): Unit = {
      rec match {
        case h :: tail =>
          val (a, col) = columns(i)
          out += f(col.asInstanceOf[C2[Any]], h, a)
          loop(i + 1, tail)
        case HNil => ()
      }
    }

    loop(0, r)
    out.toSeq
  }

  def compareRecords[C2[_], B](r1: R, r2: R, f: ColumnCompare[C2, A, B])(implicit ev: C[_] <:< C2[_]): Seq[B] = {
    val out = mutable.Buffer[B]()

    @tailrec
    def loop(i: Int, rec1: HList, rec2: HList): Unit = {
      (rec1, rec2) match {
        case (hex :: tailold, hnr :: tailnew) =>
          val (a, col) = columns(i)
          out ++= f(col.asInstanceOf[C2[Any]], hex, hnr, a)
          loop(i + 1, tailold, tailnew)
        case _ => ()
      }
    }

    loop(0, r1, r2)
    out.toSeq
  }

  def mkRecord[C2[_]](f: ColumnRetrieve[C2, A])(implicit ev: C[_] <:< C2[_]): R = {
    @tailrec
    def loop(offs: Int, l: HList): HList = {
      if (offs < 0) l
      else {
        val (a, col) = columns(offs)
        val v        = f(col, offs, a)
        loop(offs - 1, v :: l)
      }
    }

    loop(columns.length - 1, HNil).asInstanceOf[R]
  }
}

object ColumnRecord {
  def prepend[C[_], A, R1 <: HList, R2 <: HList](
      c1: ColumnRecord[C, A, R1],
      c2: ColumnRecord[C, A, R2]
  )(implicit prepend: Prepend[R1, R2]): ColumnRecord[C, A, prepend.Out] =
    ColumnRecord(c1.columns ++ c2.columns)

  def empty[C[_], A, R <: HList] = new ColumnRecord[C, A, R] {
    override def columns: Seq[(A, C[_])] = Seq.empty
  }

  def apply[C[_], A, R <: HList](cols: Seq[(A, C[_])]): ColumnRecord[C, A, R] =
    new ColumnRecord[C, A, R] {
      def columns = cols
    }

  implicit def unitColumnRecord[R <: HList, C[_], OutC <: HList](
      implicit liftAll: LiftAll.Aux[C, R, OutC],
      toList: ToList[OutC, C[_]]
  ): ColumnRecord[C, Unit, R] =
    ColumnRecord(toList(liftAll.instances).map(c => ((), c)))

}

sealed trait ColumnRetriever[C[_], A, T] {
  def columns: Seq[(A, C[_])]
  def retrieve(f: ColumnRetrieve[C, A]): T
}

case class ColumnSubset[C[_], T, Repr <: HList](columns: Seq[(String, C[_])], from: T => Repr)
    extends ColumnRecord[C, String, Repr] with ColumnRetriever[C, String, Repr] {
  override def retrieve(f: ColumnRetrieve[C, String]): Repr = mkRecord(f)
}

case class Columns[C[_], T, R <: HList](columns: Seq[(String, C[_])], iso: Iso[T, R])
    extends ColumnRecord[C, String, R] with ColumnRetriever[C, String, T] {
  override def retrieve(f: ColumnRetrieve[C, String]): T = iso.from(mkRecord(f))
  def compose[T2](ciso: Iso[T2, T]): Columns[C, T2, R] = copy(iso = ciso >>> iso)

  def subset[Keys](
      implicit ss: ColumnSubsetBuilder[R, Keys]
  ): ColumnSubset[C, R, ss.Out] = {
    val (subCols, convert) = ss.apply()
    ColumnSubset(subCols.map(colName => columns.find(_._1 == colName).get), convert)
  }

  def toSubset: ColumnSubset[C, T, R] = ColumnSubset(columns, iso.to)

  def singleColumn[A](col: Witness)(implicit selector: Selector.Aux[R, col.T, A], ev: col.T <:< Symbol): (String, C[A]) =
    columns.find(_._1 == col.value.name).get.asInstanceOf[(String, C[A])]
}

object Columns {
  def empty[C[_]]: Columns[C, HNil, HNil] = Columns(Seq.empty, Iso.id)
}

trait ColumnBuilder[C[_], T] {
  type Repr <: HList
  def apply(): Columns[C, T, Repr]
}

trait ColumnBuilderLP {
  implicit def singleColumn[C[_], K <: Symbol, V](
      implicit wk: Witness.Aux[K],
      headColumn: C[V]
  ): ColumnBuilder.Aux[C, FieldType[K, V], FieldType[K, V] :: HNil] =
    new ColumnBuilder[C, FieldType[K, V]] {
      type Repr = FieldType[K, V] :: HNil

      override def apply() =
        Columns(Seq(wk.value.name -> headColumn), Iso(_ :: HNil, _.head))
    }
}

object ColumnBuilder extends ColumnBuilderLP {
  type Aux[C[_], T, Repr0 <: HList] = ColumnBuilder[C, T] {
    type Repr = Repr0
  }

  implicit def hnilRelation[C[_]]: ColumnBuilder.Aux[C, HNil, HNil] =
    new ColumnBuilder[C, HNil] {
      type Repr = HNil

      override def apply() = Columns(Seq.empty, Iso.id)
    }

  implicit def embeddedField[C[_], K, V, Repr <: HList](
      implicit embeddedCols: ColumnBuilder.Aux[C, V, Repr]
  ): ColumnBuilder.Aux[C, FieldType[K, V], Repr] =
    embeddedCols.asInstanceOf[ColumnBuilder.Aux[C, FieldType[K, V], Repr]]

  implicit def hconsRelation[C[_],
                             H,
                             HOut <: HList,
                             HLen <: Nat,
                             T <: HList,
                             TOut <: HList,
                             Out <: HList](
      implicit
      headColumns: ColumnBuilder.Aux[C, H, HOut],
      tailColumns: ColumnBuilder.Aux[C, T, TOut],
      len: Length.Aux[HOut, HLen],
      append: Prepend.Aux[HOut, TOut, Out],
      split: Split.Aux[Out, HLen, HOut, TOut]
  ): ColumnBuilder.Aux[C, H :: T, Out] =
    new ColumnBuilder[C, H :: T] {
      type Repr = Out

      override def apply() = {
        val hc = headColumns.apply()
        val tc = tailColumns.apply()
        Columns(
          hc.columns ++ tc.columns,
          Iso(t => append.apply(hc.iso.to(t.head), tc.iso.to(t.tail)), { o =>
            val (ho, to) = split(o)
            hc.iso.from(ho) :: tc.iso.from(to)
          })
        )
      }
    }
}

trait ColumnSubsetBuilder[Repr, Keys] {
  type Out <: HList
  def apply(): (List[String], Repr => Out)
}

object ColumnSubsetBuilder {
  type Aux[Repr, Keys, Out0 <: HList] = ColumnSubsetBuilder[Repr, Keys] {
    type Out = Out0
  }

  implicit def isSubset[Repr <: HList, K <: HList, KOut <: HList, Out0 <: HList, RecOut <: HList](
      implicit sa: SelectAll.Aux[Repr, K, Out0],
      withKeys: ZipWithKeys.Aux[K, Out0, RecOut],
      keys: Keys.Aux[RecOut, KOut],
      toList: ToList[KOut, Symbol]
  ): ColumnSubsetBuilder.Aux[Repr, K, Out0] =
    new ColumnSubsetBuilder[Repr, K] {
      type Out = Out0

      def apply() = (toList(keys()).map(_.name), sa.apply)
    }
}
