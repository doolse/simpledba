package io.doolse.simpledba

import shapeless._
import shapeless.labelled.FieldType

/**
  * Created by jolz on 5/05/16.
  */

abstract class RelationMapper {
  type IO[A]
  type ResultSetOps[A]
  type CT[A]
  type DDL

  val relIO: RelationIO.Aux[IO, ResultSetOps, CT]

  case class PhysicalColumn[T](meta: ColumnMetadata, column: ColumnAtom[T])

  type AtomIndexed[T] = FieldType[_, ColumnAtom[T]]

  trait ColumnAtom[FT] {
    type ColType

    def column: CT[ColType]
  }

  object ColumnAtom {
    type Aux[FT, T0] = ColumnAtom[FT] {
      type T = T0
    }
  }

  case class TableBuilder[T, Keys <: HList, SortKeys <: HList](name: String) {
    def key(key: Witness) = copy[T, key.T :: HNil, SortKeys]()

    def andKey(key: Witness) = copy[T, key.T :: Keys, SortKeys]()
  }

  trait PhysicalTableBuilder[T, Keys <: HList, SortKeys <: HList] extends DepFn1[TableBuilder[T, Keys, SortKeys]] {
    type Out <: PhysicalTable[T]
  }

  trait KeySelector[Columnizer, Keys] {
    type ColumnsRecord <: HList
    type FullKeyValue <: HList
    type PartialKeyValue <: HList

    def transform(col: Columnizer): ColumnsRecord

    def fullKeyColumns: List[ColumnName]

    def partialKeyColumns: List[ColumnName]

    def fullKeyParameters(fkv: FullKeyValue): Iterable[relIO.QP[Any]]

    def partialKeyParameters(fkv: PartialKeyValue): Iterable[relIO.QP[Any]]
  }

  object KeySelector {
    type Aux[Columnizer, Keys, CR0 <: HList, FKV0 <: HList, PKV0 <: HList] = KeySelector[Columnizer, Keys] {
      type ColumnsRecord = CR0
      type FullKeyValue = FKV0
      type PartialKeyValue = PKV0
    }
  }

  trait PhysicalTable[T] {
    def name: String

    val keySelector: KeySelector[_, _]

    def columns: List[PhysicalColumn[_]]

    def fromResultSet: ResultSetOps[T]

    def genDDL: DDL
  }

  trait Columnizer[T] {
    type ColumnsRecord <: HList
    type Values <: HList

    def columns: ColumnsRecord

    def fromValues: Values => T
  }

  object Columnizer {
    type Aux[T, CR0 <: HList, V0 <: HList] = Columnizer[T] {
      type ColumnsRecord = CR0
      type Values = V0
    }

    implicit def lgColumnizer[T, Repr <: HList](implicit lg: LabelledGeneric.Aux[T, Repr], columnizer: Columnizer[Repr]) = new Columnizer[T] {
      type ColumnsRecord = columnizer.ColumnsRecord
      type Values = columnizer.Values

      def fromValues: Values => T = ???

      def columns: columnizer.ColumnsRecord = columnizer.columns
    }

    implicit def hnilColumnizer = new Columnizer[HNil] {
      type ColumnsRecord = HNil
      type Values = HNil

      def fromValues: (HNil) => HNil = identity

      def columns: HNil = HNil
    }

    implicit def atomColumnizer[S <: Symbol, H, T <: HList](implicit atom: ColumnAtom[H], tailCols: Columnizer[T]) = new Columnizer[FieldType[S, H] :: T] {
      type ColumnsRecord = FieldType[S, ColumnAtom[H]] :: tailCols.ColumnsRecord

      def fromValues: Values => FieldType[S, H] :: T = ???

      type Values = H :: tailCols.Values

      def columns: FieldType[S, ColumnAtom[H]] :: tailCols.ColumnsRecord = labelled.field[S](atom) :: tailCols.columns
    }
  }

  def table[T](name: String) = TableBuilder[T, HNil, HNil](name)

  def generateDDL[T, Keys <: HList, SortKeys <: HList](tableBuilder: TableBuilder[T, Keys, SortKeys])
                                                      (implicit gen: PhysicalTableBuilder[T, Keys, SortKeys]): DDL =
    gen(tableBuilder).genDDL
}

