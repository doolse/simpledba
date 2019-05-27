package io.doolse.simpledba

import cats.arrow.Compose
import cats.instances.function._
import cats.syntax.compose._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Length, LiftAll, Prepend, Split, ToList, ZipWithKeys}
import shapeless.ops.record.{Keys, SelectAll}
import shapeless.{::, HList, HNil, Nat, Witness}

import scala.annotation.tailrec
import scala.collection.mutable

case class Iso[A, B](to: A => B, from: B => A)

object Iso {
  def id[A]: Iso[A, A] = Iso(identity, identity)

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

trait ColumnRetrieve[C[_], A] {
  def apply[V](column: C[V], offset: Int, a: A): V
}

sealed trait ColumnRecord[C[_], A, R <: HList] {
  def columns: Seq[(A, C[_])]
  def mapRecord[B](r: R, f: ColumnMapper[C, A, B]): Seq[B] = {
    val out = mutable.Buffer[B]()

    @tailrec
    def loop(i: Int, rec: HList): Unit = {
      rec match {
        case h :: tail =>
          val (a, col: C[Any]) = columns(i)
          out += f(col, h, a)
          loop(i + 1, tail)
        case HNil => ()
      }
    }

    loop(0, r)
    out
  }

  def mkRecord(f: ColumnRetrieve[C, A]): R = {
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

sealed trait ColumnRecordIso[C[_], A, R <: HList, T] extends ColumnRecord[C, A, R] {
  def iso: Iso[T, R]
  def retrieve(f: ColumnRetrieve[C, A]): T = iso.from(mkRecord(f))
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

case class ColumnSubset[C[_], R, T, Repr <: HList](columns: Seq[(String, C[_])], iso: Iso[T, Repr])
    extends ColumnRecordIso[C, String, Repr, T]

object ColumnSubset {
  def empty[C[_], R] = ColumnSubset[C, R, HNil, HNil](Seq.empty, Iso.id)
}

case class Columns[C[_], T, R <: HList](columns: Seq[(String, C[_])], iso: Iso[T, R])
    extends ColumnRecordIso[C, String, R, T] {
  def compose[T2](ciso: Iso[T2, T]): Columns[C, T2, R] = copy(iso = ciso >>> iso)

  def subset[Keys](
      implicit ss: ColumnSubsetBuilder[R, Keys]
  ): (ColumnSubset[C, R, ss.Out, ss.Out], R => ss.Out) = {
    val (subCols, convert) = ss.apply()
    (ColumnSubset(subCols.map(colName => columns.find(_._1 == colName).get), Iso.id), convert)
  }
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
  type Aux[Repr, Keys, Out0] = ColumnSubsetBuilder[Repr, Keys] {
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
