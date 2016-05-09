package io.doolse.simpledba

import cats.Monad
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist._
import shapeless.ops.record.{Fields, Keys, SelectAll, Values}
import cats.syntax.all._

/**
  * Created by jolz on 5/05/16.
  */

abstract class RelationMapper[IO[_] : Monad, RSOps[_]](implicit RM: Monad[RSOps]) {
  type CT[A]
  type DDL

  val connection: RelationIO.Aux[IO, RSOps, CT]
  val resultSet: ResultSetOps[RSOps, CT]

  type QP = connection.QP

  case class PhysicalColumn[T](meta: ColumnMetadata, column: ColumnAtom[T])

  type AtomRecord[T] = (_ <: Symbol, ColumnAtom[T])

  trait ColumnAtom[FT] {
    type ColType

    def column: CT[ColType]

    def parameter(t: FT): QP

    def fromResults(colRef: ColumnReference): RSOps[FT]
  }

  object ColumnAtom {
    type Aux[FT, T0] = ColumnAtom[FT] {
      type T = T0
    }

    implicit def stdAtom[T](implicit stdCol: CT[T]): ColumnAtom[T] = new ColumnAtom[T] {
      type ColType = T

      def column = stdCol

      def parameter(t: T): QP = connection.parameter(stdCol, t)

      def fromResults(colRef: ColumnReference): RSOps[T] = resultSet.getColumn(colRef, stdCol).map(_.getOrElse(sys.error("Data is bad")))
    }

  }

  object atomToResultOp extends Poly {
    implicit def recordToResults[S <: Symbol, T, L <: HList] = use { (t: (S, ColumnAtom[T]), rs: RSOps[L]) =>
      rs.flatMap(l => t._2.fromResults(ColumnName(t._1.name)).map(c => labelled.field[S](c) :: l))
    }
  }


  trait KeyQuery[KeyValue <: HList] {
    def columnNames: List[ColumnName]

    def queryParameters(fkv: KeyValue): Iterable[QP]
  }

  trait KeyQueryBuilder[TR, K] extends DepFn1[TR]

  object KeyQueryBuilder {
    type Aux[TR, K, V0 <: HList] = KeyQueryBuilder[TR, K] {
      type Out = KeyQuery[V0]
    }

    object toQueryParameter extends Poly1 {
      implicit def allCols[T0] = at[(T0, ColumnAtom[T0])](c => c._2.parameter(c._1) )
    }

    implicit def fromKeyList[TR <: HList, K <: HList, KC <: HList, KS <: HList, KO <: HList, Raw <: HList, ZV <: HList, MZ <: HList]
    (implicit select: SelectAll.Aux[TR, K, KC],
     zipWithKeys: ZipWithKeys.Aux[K, KC, KS],
     keys: Keys.Aux[KS, KO],
     withoutAtom: Comapped.Aux[KC, ColumnAtom, Raw],
     zippedVals: Zip.Aux[Raw :: KC :: HNil, ZV],
     mappedZipped: Mapper.Aux[toQueryParameter.type, ZV, MZ],
     toListZip: ToList[MZ, QP],
     toList: ToList[KO, Symbol]) = new KeyQueryBuilder[TR, K] {
      type Out = KeyQuery[Raw]

      def apply(t: TR): KeyQuery[Raw] = new KeyQuery[Raw] {
        def columnNames: List[ColumnName] = toList(keys()).map(s => ColumnName(s.name))

        def queryParameters(fkv: Raw): Iterable[QP] = {
          toListZip(mappedZipped.apply(zippedVals(fkv :: select(t) :: HNil)))
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
    CR <: HList, FKV <: HList, PKV <: HList, PhysFields <: HList, CRV <: HList, AllKeys <: HList,
    CRVO <: HList]
    (implicit
     columnizer: Columnizer.Aux[T, TR, V],
     keySelect: KeySelector.Aux[T, TR, V, (K, SK), CR, CRV, FKV, PKV],
     crValsOnly: Values.Aux[CRV, CRVO],
     allColumnKeys: Keys.Aux[CR, AllKeys],
     insertQP: KeyQueryBuilder.Aux[CR, AllKeys, CRVO],
     fields: Fields.Aux[CR, PhysFields],
     toResMapper: RightFolder.Aux[PhysFields, RSOps[HNil], atomToResultOp.type, RSOps[CRV]],
     toList: ToList[PhysFields, (_ <: Symbol, ColumnAtom[_])])
    = new PhysicalTableBuilder[T, K, SK] {
      type Out = PhysicalTable[T, FKV, PKV]

      val (origColumns, from, to) = columnizer()

      def apply(t: TableBuilder[T, K, SK]): PhysicalTable[T, FKV, PKV] = {

        val (allCols, fullKeyQ, partialKeyQ, c, toQP) = keySelect(origColumns)
        val allVals = insertQP(allCols)

        new PhysicalTable[T, FKV, PKV] {
          def name: String = t.name

          def fromResultSet: RSOps[T] = {
            fields(allCols).foldRight(RM.pure(HNil : HNil))(atomToResultOp).map(c andThen from)
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

          def parameters(t: T): Iterable[QP] = allVals.queryParameters(crValsOnly(toQP(to(t))))
        }
      }
    }

  }

  trait KeySelector[T, TR, V, Keys] extends DepFn1[TR]

  object KeySelector {
    type Aux[T, TR <: HList, V <: HList, Keys, CR0 <: HList, OutVals <: HList, FKV0 <: HList, PKV0 <: HList] = KeySelector[T, TR, V, Keys] {
      type Out = (CR0, KeyQuery[FKV0], KeyQuery[PKV0], OutVals => V, V => OutVals)
    }
  }

  trait PhysicalTable[T, FullKeyValue <: HList, PartialKeyValue <: HList] {
    def name: String

    val fullKey: KeyQuery[FullKeyValue]

    val partialKey: KeyQuery[PartialKeyValue]

    def columns: List[PhysicalColumn[_]]

    def parameters(t: T): Iterable[QP]

    def fromResultSet: RSOps[T]

    lazy val allColumns: List[ColumnName] = columns.map(c => ColumnName(c.meta.name))

  }

  trait Columnizer[T] extends DepFn0

  object Columnizer {
    type Aux[T, CR0 <: HList, Vals0 <: HList] = Columnizer[T] {
      type Out = (CR0, Vals0 => T, T => Vals0)
    }

    implicit def lgColumnizer[T, ColRecord <: HList, ColVals <: HList]
    (implicit lg: LabelledGeneric.Aux[T, ColVals], columnizer: Columnizer.Aux[ColVals, ColRecord, ColVals]) = new Columnizer[T] {
      type Out = (ColRecord, ColVals => T, T => ColVals)

      def apply(): Out = {
        val (c, from, to) = columnizer()
        (c, lg.from, lg.to)
      }
    }

    implicit def hnilColumnizer = new Columnizer[HNil] {
      type Out = (HNil, HNil => HNil, HNil => HNil)

      def apply(): (HNil, HNil => HNil, HNil => HNil) = (HNil, identity, identity)
    }

    implicit def atomColumnizer[S <: Symbol, H, T <: HList, TCR <: HList, TVals <: HList]
    (implicit atom: ColumnAtom[H], tailCols: Columnizer.Aux[T, TCR, TVals]) = new Columnizer[FieldType[S, H] :: T] {
      type ColumnsRecord = FieldType[S, ColumnAtom[H]] :: TCR
      type Vals = FieldType[S, H] :: TVals
      type OutVals = FieldType[S, H] :: T
      type Out = (ColumnsRecord, Vals => OutVals, OutVals => Vals)

      def apply() : Out = {
        val (cr, from, to) = tailCols()
        (labelled.field[S](atom) :: cr, (tcr:Vals) => tcr.head :: from(tcr.tail), (outv:OutVals) => outv.head :: to(outv.tail))
      }
    }
  }

  def table[T](name: String) = TableBuilder[T, HNil, HNil](name)

  def physicalTable[T, Keys <: HList, SortKeys <: HList, FKV <: HList, PKV <: HList](tableBuilder: TableBuilder[T, Keys, SortKeys])
                                                                                    (implicit gen: PhysicalTableBuilder.Aux[T, Keys, SortKeys, FKV, PKV]): PhysicalTable[T, FKV, PKV] = gen(tableBuilder)
  def genDDL(physicalTable: PhysicalTable[_, _, _]): DDL

  def queries[T, FullKey <: HList, PartialKey <: HList](physicalTable: PhysicalTable[T, FullKey, PartialKey]): RelationQueries[IO, T, FullKey, PartialKey] = {

    lazy val insQ = InsertQuery(physicalTable.name, physicalTable.allColumns)
    lazy val selectAllQ = SelectQuery(physicalTable.name, physicalTable.allColumns, physicalTable.fullKey.columnNames, None)

    def insert(t: T): IO[Unit] = {
      connection.query(insQ, physicalTable.parameters(t)).map(_ => ())
    }
    def update(existing: T, updated: T): IO[Boolean] = ???
    def queryByKey(key: FullKey): IO[Option[T]] = {
      val rsOps : RSOps[Option[T]] = for {
        y <- resultSet.nextResult
        res <- if (y) physicalTable.fromResultSet.map(Option(_)) else RM.pure(None)
      } yield res
      connection.query(selectAllQ, physicalTable.fullKey.queryParameters(key)).flatMap(rs => connection.usingResults(rs, rsOps))
    }
    def deleteByPartKey(partialKey: PartialKey): IO[Unit] = ???
    def delete(key: FullKey): IO[Unit] = ???
    def deleteByValue(t: T): IO[Unit] = ???
    RelationQueries[IO, T, FullKey, PartialKey](insert, update, queryByKey, deleteByPartKey, delete, deleteByValue)
  }
}
