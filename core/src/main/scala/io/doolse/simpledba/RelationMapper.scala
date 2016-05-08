package io.doolse.simpledba

import cats.Monad
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist._
import shapeless.ops.record.{Fields, Keys, SelectAll}
import cats.syntax.all._

/**
  * Created by jolz on 5/05/16.
  */

abstract class RelationMapper[IO[_], ResultSetOps[_]](implicit RM: Monad[ResultSetOps]) {
  type CT[A]
  type DDL

  val relIO: RelationIO.Aux[IO, ResultSetOps, CT]

  case class PhysicalColumn[T](meta: ColumnMetadata, column: ColumnAtom[T])

  type AtomRecord[T] = (_ <: Symbol, ColumnAtom[T])

  type AtomWithValue[T] = (T, ColumnAtom[T])

  trait ColumnAtom[FT] {
    type ColType

    def column: CT[ColType]

    def parameter(t: FT): relIO.QP[Any]

    def fromResults(colRef: ColumnReference): ResultSetOps[FT]
  }

  object ColumnAtom {
    type Aux[FT, T0] = ColumnAtom[FT] {
      type T = T0
    }

    implicit def stdAtom[T](implicit stdCol: CT[T]): ColumnAtom[T] = new ColumnAtom[T] {
      type ColType = T

      def column = stdCol

      def parameter(t: T): relIO.QP[Any] = relIO.parameter(stdCol, t).asInstanceOf[relIO.QP[Any]]

      def fromResults(colRef: ColumnReference): ResultSetOps[T] = relIO.resultSetOperations.getColumn(colRef, stdCol).map(_.getOrElse(sys.error("Data is bad")))
    }

  }

  object atomToResultOp extends Poly {
    implicit def recordToResults[S <: Symbol, T, L <: HList] = use { (t: (S, ColumnAtom[T]), rs: ResultSetOps[L]) =>
      rs.flatMap(l => t._2.fromResults(ColumnName(t._1.name)).map(c => labelled.field[S](c) :: l))
    }
  }


  trait KeyQuery[KeyValue <: HList] {
    def columnNames: List[ColumnName]

    def queryParameters(fkv: KeyValue): Iterable[relIO.QP[Any]]
  }

  trait KeyQueryBuilder[TR, K] extends DepFn1[TR]

  object KeyQueryBuilder {
    type Aux[TR, K, V0 <: HList] = KeyQueryBuilder[TR, K] {
      type Out = KeyQuery[V0]
    }

    implicit def fromKeyList[TR <: HList, K <: HList, KC <: HList, KS <: HList, KO <: HList, Raw <: HList, ZV <: HList]
    (implicit select: SelectAll.Aux[TR, K, KC],
     zipWithKeys: ZipWithKeys.Aux[K, KC, KS],
     keys: Keys.Aux[KS, KO],
     withoutAtom: Comapped.Aux[KC, ColumnAtom, Raw],
     zippedVals: Zip.Aux[Raw :: KC :: HNil, ZV],
     toListZip: ToList[ZV, AtomWithValue[_]],
     toList: ToList[KO, Symbol]) = new KeyQueryBuilder[TR, K] {
      type Out = KeyQuery[Raw]

      def apply(t: TR): KeyQuery[Raw] = new KeyQuery[Raw] {
        def columnNames: List[ColumnName] = toList(keys()).map(s => ColumnName(s.name))

        def queryParameters(fkv: Raw): Iterable[relIO.QP[Any]] = {
          toListZip(zippedVals(fkv :: select(t) :: HNil)).map {
            case (v, cat) => cat.parameter(v)
          }
        }
      }
    }
  }

  case class TableBuilder[T, Keys <: HList, SortKeys <: HList](name: String) {
    def key(key: Witness) = copy[T, key.T :: HNil, SortKeys]()

    def andKey(key: Witness) = copy[T, key.T :: Keys, SortKeys]()
  }

  trait PhysicalTableBuilder[T, Keys <: HList, SortKeys <: HList] extends DepFn1[TableBuilder[T, Keys, SortKeys]]

  object PhysicalTableBuilder {
    type Aux[T, Keys <: HList, SortKeys <: HList, FKV0 <: HList, PKV0 <: HList] = PhysicalTableBuilder[T, Keys, SortKeys] {
      type Out = PhysicalTable[T, FKV0, PKV0]
    }

    implicit def physicalTable[T, K <: HList, SK <: HList, TR <: HList, V <: HList,
    CR <: HList, FKV <: HList, PKV <: HList, PhysFields <: HList, CRV <: HList]
    (implicit
     columnizer: Columnizer.Aux[T, TR, V],
     keySelect: KeySelector.Aux[T, TR, V, (K, SK), CR, CRV, FKV, PKV],
     fields: Fields.Aux[CR, PhysFields],
     toResMapper: RightFolder.Aux[PhysFields, ResultSetOps[HNil], atomToResultOp.type, ResultSetOps[CRV]],
     toList: ToList[PhysFields, (_ <: Symbol, ColumnAtom[_])])
    = new PhysicalTableBuilder[T, K, SK] {
      type Out = PhysicalTable[T, FKV, PKV]

      val (origColumns, f) = columnizer()
      def apply(t: TableBuilder[T, K, SK]): PhysicalTable[T, FKV, PKV] = {
        val (allCols, fullKeyQ, partialKeyQ, c) = keySelect(origColumns)
        new PhysicalTable[T, FKV, PKV] {
          def name: String = t.name

          def fromResultSet: ResultSetOps[T] = {
            fields(allCols).foldRight(RM.pure(HNil : HNil))(atomToResultOp).map(c andThen f)
          }

          lazy val columns: List[PhysicalColumn[_]] = {
            val partitionFields = partialKeyQ.columnNames.map(_.name).toSet
            val fullFields = fullKeyQ.columnNames.map(_.name).toSet
            toList(fields(allCols)).map {
              case (Symbol(s), cat) => PhysicalColumn(ColumnMetadata(s,
                if (partitionFields.contains(s)) PartitionKey else if (fullFields.contains(s)) SortKey else StandardColumn), cat)
            }
          }

          val fullKey = fullKeyQ
          val partialKey = partialKeyQ
        }
      }
    }

  }

  trait KeySelector[T, TR, V, Keys] extends DepFn1[TR]

  object KeySelector {
    type Aux[T, TR <: HList, V <: HList, Keys, CR0 <: HList, OutVals <: HList, FKV0 <: HList, PKV0 <: HList] = KeySelector[T, TR, V, Keys] {
      type Out = (CR0, KeyQuery[FKV0], KeyQuery[PKV0], OutVals => V)
    }
  }

  trait PhysicalTable[T, FullKeyValue <: HList, PartialKeyValue <: HList] {
    def name: String

    val fullKey: KeyQuery[FullKeyValue]

    val partialKey: KeyQuery[PartialKeyValue]

    def columns: List[PhysicalColumn[_]]

    def fromResultSet: ResultSetOps[T]

    lazy val allColumns: List[ColumnName] = columns.map(c => ColumnName(c.meta.name))

  }

  trait Columnizer[T] extends DepFn0

  object Columnizer {
    type Aux[T, CR0 <: HList, Vals0 <: HList] = Columnizer[T] {
      type Out = (CR0, Vals0 => T)
    }

    implicit def lgColumnizer[T, Repr <: HList, ColRecord <: HList, ColVals <: HList]
    (implicit lg: LabelledGeneric.Aux[T, Repr], columnizer: Columnizer.Aux[Repr, ColRecord, ColVals]) = new Columnizer[T] {
      type Out = (ColRecord, ColVals => T)

      def apply(): (ColRecord, ColVals => T) = {
        val (c, f) = columnizer()
        (c, f andThen lg.from)
      }
    }

    implicit def hnilColumnizer = new Columnizer[HNil] {
      type Out = (HNil, HNil => HNil)

      def apply(): (HNil, HNil => HNil) = (HNil, identity)
    }

    implicit def atomColumnizer[S <: Symbol, H, T <: HList, TCR <: HList, TVals <: HList](implicit atom: ColumnAtom[H], tailCols: Columnizer.Aux[T, TCR, TVals]) = new Columnizer[FieldType[S, H] :: T] {
      type ColumnsRecord = FieldType[S, ColumnAtom[H]] :: TCR
      type Vals = FieldType[S, H] :: TVals
      type Out = (ColumnsRecord, Vals => FieldType[S, H] :: T)

      def apply() = {
        val (cr, f) = tailCols()
        (labelled.field[S](atom) :: cr, tcr => tcr.head :: f(tcr.tail))
      }
    }
  }

  def table[T](name: String) = TableBuilder[T, HNil, HNil](name)

  def physicalTable[T, Keys <: HList, SortKeys <: HList, FKV <: HList, PKV <: HList](tableBuilder: TableBuilder[T, Keys, SortKeys])
                                                                                    (implicit gen: PhysicalTableBuilder.Aux[T, Keys, SortKeys, FKV, PKV]): PhysicalTable[T, FKV, PKV] = gen(tableBuilder)
  def genDDL(physicalTable: PhysicalTable[_, _, _]): DDL

}
