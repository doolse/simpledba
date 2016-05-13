package io.doolse.simpledba

import cats.data.Xor
import cats.{Applicative, Monad}
import cats.sequence.{Traverser, _}
import cats.syntax.all._
import cats.std.option._
import shapeless.labelled._
import shapeless._
import shapeless.ops.hlist.{Comapped, Drop, Length, Mapped, Mapper, Prepend, Take, ToList, ZipConst, ZipWithKeys}
import shapeless.ops.record._
import shapeless.UnaryTCConstraint._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 10/05/16.
  */
abstract class Mapper2[F[_] : Monad, RSOps[_] : Monad, PhysCol[_]](val connection: RelationIO.Aux[F, RSOps, PhysCol]) {
  type DDL[A]
  type QueryParam = connection.QP

  val resultset = connection.rsOps

  def DDLMonad: Monad[DDL]

  def build[A](ddl: DDL[A]): A

  trait ColumnAtom[A] {
    def getColumn(cr: ColumnReference): RSOps[Option[A]]

    def queryParameter(a: A): QueryParam
  }

  object ColumnAtom {
    implicit def stdColumn[A](implicit col: PhysCol[A], tag: ClassTag[A]) = new ColumnAtom[A] {
      def getColumn(cr: ColumnReference): RSOps[Option[A]] = connection.rsOps.getColumn(cr, col)

      def queryParameter(a: A): QueryParam = connection.parameter(col, a)

      override def toString = tag.runtimeClass.getName
    }
  }

  case class ColumnMapping[S, A](name: ColumnName, atom: ColumnAtom[A], get: S => A)(implicit tt: TypeTag[S]) {
    override def toString = s"('${name.name}',$atom,$tt)"
  }

  //  trait ColumnValuesType[L <: HList] {
  //    type Out <: HList
  //  }
  //
  //  object ColumnValuesType {
  //    type Aux[L <: HList, Out0 <: HList] = ColumnValuesType[L] { type Out = Out0 }
  //    implicit val hnilColumnType = new ColumnValuesType[HNil] {
  //      type Out = HNil
  //    }
  //
  //    implicit def hconsColumnType[S, K, V, T <: HList](implicit tailTypes: ColumnValuesType[T])
  //    = new ColumnValuesType[(FieldType[K, ColumnMapping[S, V]]) :: T] {
  //      type Out = V :: tailTypes.Out
  //    }
  //  }

  trait ToQueryParameters[Columns <: HList] {
    type QueryValues <: HList

    def parameters(columns: Columns): QueryValues => Iterable[QueryParam]
  }

  object ToQueryParameters {
    type Aux[Columns <: HList, QV0 <: HList] = ToQueryParameters[Columns] {type QueryValues = QV0}
    implicit val hnilParams = new ToQueryParameters[HNil] {
      type QueryValues = HNil

      def parameters(col: HNil) = _ => Iterable.empty
    }

    implicit def hconsParams[V, S, T <: HList]
    (implicit
     tail: ToQueryParameters[T]): Aux[ColumnMapping[S, V] :: T, V :: tail.QueryValues]
    = new ToQueryParameters[ColumnMapping[S, V] :: T] {
      type QueryValues = V :: tail.QueryValues

      def parameters(cols: ColumnMapping[S, V] :: T) = {
        qv => Iterable(cols.head.atom.queryParameter(qv.head)) ++ tail.parameters(cols.tail).apply(qv.tail)
      }
    }
  }

  trait ColumnNames[Columns <: HList] extends (Columns => List[ColumnName])

  object ColumnNames {
    implicit def columnNames[L <: HList, LM <: HList](implicit mapper: Mapper.Aux[columnNamesFromMappings.type, L, LM], toList: ToList[LM, ColumnName]) = new ColumnNames[L] {
      def apply(columns: L): List[ColumnName] = toList(mapper(columns))
    }
  }

  trait ColumnsAsRS[Columns <: HList] {
    type ColumnsValues

    def toRS(cols: Columns): RSOps[Option[ColumnsValues]]
  }

  object ColumnsAsRS {
    type Aux[Columns <: HList, ColumnsValues0] = ColumnsAsRS[Columns] {type ColumnsValues = ColumnsValues0}

    implicit def resultSet[L <: HList, VWithOps <: HList, CV <: HList]
    (implicit app: Applicative[Option],
     traverser: Traverser.Aux[L, columnMappingToRS.type, RSOps[VWithOps]],
     sequenceOps: Sequencer.Aux[VWithOps, Option[CV]]
    ) = new ColumnsAsRS[L] {
      type ColumnsValues = CV

      def toRS(cols: L): RSOps[Option[CV]] = traverser(cols).map(opV => sequenceOps(opV))
    }
  }

  object columnNamesFromMappings extends Poly1 {
    implicit def mappingToName[S, A] = at[ColumnMapping[S, A]](_.name)
  }

  object columnMappingToRS extends Poly1 {
    implicit def mappingToRS[S, A] = at[ColumnMapping[S, A]](cm => cm.atom.getColumn(cm.name))
  }

  object composeLens extends Poly1 {
    implicit def convertLens[T, T2, K, A](implicit tt: TypeTag[T]) = at[(FieldType[K, ColumnMapping[T2, A]], T => T2)] {
      case (colMapping, lens) => field[K](colMapping.copy[T, A](get = colMapping.get compose lens))
    }
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] {
    type Columns <: HList
    type ColumnsValues <: HList

    def columns: Columns

    def fromColumns: ColumnsValues => A
    def toColumns: A => ColumnsValues
  }

  object ColumnMapper {
    type Aux[A, Columns0 <: HList, ColumnsValues0 <: HList] = ColumnMapper[A] {
      type Columns = Columns0
      type ColumnsValues = ColumnsValues0
    }

    implicit val hnilMapper: ColumnMapper.Aux[HNil, HNil, HNil] = new ColumnMapper[HNil] {
      type Columns = HNil
      type ColumnsValues = HNil

      def columns = HNil

      def fromColumns = identity
      def toColumns = identity
    }

    implicit def singleColumn[K <: Symbol, V](implicit atom: ColumnAtom[V], key: Witness.Aux[K], tt: TypeTag[FieldType[K, V]]):
    ColumnMapper.Aux[FieldType[K, V], FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil, V :: HNil] = new ColumnMapper[FieldType[K, V]] {
      type Columns = FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil
      type ColumnsValues = V :: HNil

      def columns = field[K](ColumnMapping[FieldType[K, V], V](ColumnName(key.value.name), atom, fv => fv: V)) :: HNil

      def fromColumns = v => field[K](v.head)
      def toColumns = _ :: HNil
    }


    implicit def multiColumn[K <: Symbol, V, Columns0 <: HList, ColumnsValues0 <: HList, CZ <: HList]
    (implicit mapping: ColumnMapper.Aux[V, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[FieldType[K, V] => V, Columns0, CZ], mapper: Mapper[composeLens.type, CZ]) = new ColumnMapper[FieldType[K, V]] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def columns = mapper(zipWithLens(v => v: V, mapping.columns))

      def fromColumns = v => field[K](mapping.fromColumns(v))
      def toColumns = mapping.toColumns
    }

    implicit def hconsMapper[H, T <: HList,
    HC <: HList, HCZ <: HList, HCM <: HList,
    TC <: HList, TCZ <: HList, TCM <: HList,
    HV <: HList, TV <: HList,
    OutV <: HList, LenHV <: Nat]
    (implicit headMapper: ColumnMapper.Aux[H, HC, HV], tailMapper: ColumnMapper.Aux[T, TC, TV], tt: TypeTag[T],
     zipHeadLens: ZipConst.Aux[H :: T => H, HC, HCZ], zipTailLens: ZipConst.Aux[H :: T => T, TC, TCZ],
     mappedHead: Mapper.Aux[composeLens.type, HCZ, HCM], mappedTail: Mapper.Aux[composeLens.type, TCZ, TCM],
     prependC: Prepend[HCM, TCM], prependV: Prepend.Aux[HV, TV, OutV],
     lenH: Length.Aux[HV, LenHV], headVals: Take.Aux[OutV, LenHV, HV], tailVals: Drop.Aux[OutV, LenHV, TV])
    : ColumnMapper.Aux[H :: T, prependC.Out, OutV] = {
      new ColumnMapper[H :: T] {
        type Columns = prependC.Out
        type ColumnsValues = OutV

        def columns = prependC(mappedHead(zipHeadLens(_.head, headMapper.columns)), mappedTail(zipTailLens(_.tail, tailMapper.columns)))

        def fromColumns = v => headMapper.fromColumns(headVals(v)) :: tailMapper.fromColumns(tailVals(v))
        def toColumns = v => prependV(headMapper.toColumns(v.head), tailMapper.toColumns(v.tail))
      }
    }
  }

  trait GenericColumnMapper[T] {
    type Columns <: HList
    type ColumnsValues <: HList

    def apply(): ColumnMapper.Aux[T, Columns, ColumnsValues]
  }

  object GenericColumnMapper {
    def apply[T](implicit genMapper: GenericColumnMapper[T]): ColumnMapper.Aux[T, genMapper.Columns, genMapper.ColumnsValues] = genMapper.apply()

    type Aux[T, Columns0 <: HList, ColumnsValues0 <: HList] = GenericColumnMapper[T] {
      type Columns = Columns0
      type ColumnsValues = ColumnsValues0
    }

    implicit def genericColumn[T, Repr <: HList, Columns0 <: HList, ColumnsValues0 <: HList, ColumnsZ <: HList]
    (implicit
     lgen: LabelledGeneric.Aux[T, Repr],
     mapping: ColumnMapper.Aux[Repr, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[T => Repr, Columns0, ColumnsZ], mapper: Mapper[composeLens.type, ColumnsZ]
    ): Aux[T, mapper.Out, ColumnsValues0]
    = new GenericColumnMapper[T] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def apply() = new ColumnMapper[T] {
        type Columns = mapper.Out
        type ColumnsValues = ColumnsValues0

        def columns = mapper(zipWithLens(lgen.to, mapping.columns))

        def fromColumns = v => lgen.from(mapping.fromColumns(v))
        def toColumns = v => mapping.toColumns(lgen.to(v))
      }
    }
  }

  case class SingleQuery[T, KeyValues](query: KeyValues => F[Option[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  case class MultiQuery[T, KeyValues](query: KeyValues => F[Option[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  case class WriteQueries[T](insert: T => F[Unit], update: (T, T) => F[Boolean], delete: T => F[Unit])

  case class ColumnDifference(name: ColumnName, qp: QueryParam, oldValue: QueryParam)

  trait RelationOperations[T, Key] {
    def tableName: String

    def fromResultSet: RSOps[Option[T]]

    def allColumns: List[ColumnName]

    def keyColumns: List[ColumnName]

    def keyParameters(key: Key): Iterable[QueryParam]

    def keyParametersFromValue(value: T) = keyParameters(keyFromValue(value))

    def keyFromValue(value: T): Key

    def diff(value1: T, value2: T): Xor[Iterable[QueryParam], (List[ColumnDifference], Iterable[QueryParam])]

    def allParameters(value: T): Iterable[QueryParam]
  }

  def getRelationsForBuilder[T](forBuilder: RelationBuilder[T, _, _, _]): DDL[List[RelationOperations[T, _]]]

  trait PhysicalMapping[T, Columns <: HList, ColumnsValues <: HList, Keys <: HList, SelectedKeys]
    extends DepFn1[RelationBuilder[T, Columns, ColumnsValues, Keys]] {
    type KeyValues
    type Out = DDL[RelationOperations[T, KeyValues]]
  }

  object PhysicalMapping {
    type Aux[T, Columns <: HList, ColumnsValues <: HList, Keys <: HList, SelectedKeys, KeyValues0] =
    PhysicalMapping[T, Columns, ColumnsValues, Keys, SelectedKeys] {type KeyValues = KeyValues0}
  }

  case class RelationBuilder[T, Columns <: HList, ColumnsValues <: HList, Keys <: HList]
  (baseName: String, mapper: ColumnMapper.Aux[T, Columns, ColumnsValues])(implicit appOp: Applicative[Option]) {

    def queryByKey[KeyValues <: HList]
    (implicit
     physicalMapping: PhysicalMapping.Aux[T, Columns, ColumnsValues, Keys, Keys, KeyValues])
    : DDL[SingleQuery[T, KeyValues]]
    = DDLMonad.map(physicalMapping(this)) { op =>
      val selectOne = new SelectQuery(op.tableName, op.allColumns, op.keyColumns)
      SingleQuery { vals =>
        connection.query(selectOne, op.keyParameters(vals)).flatMap {
          rs =>
            val getResult: RSOps[Option[T]] = for {
              y <- resultset.nextResult
              res <- if (y) op.fromResultSet else Monad[RSOps].pure(None)
            } yield res
            connection.usingResults(rs, getResult)
        }
      }
    }


    def queryAllByKeyColumns[K <: HList](k: K) = ???

    def writeQueries: DDL[WriteQueries[T]] = DDLMonad.map(getRelationsForBuilder(this)) { relations =>
      val queries = relations.map { relation =>
        val insertQuery = InsertQuery(relation.tableName, relation.allColumns)
        val deleteQuery = DeleteQuery(relation.tableName, relation.keyColumns)
        def insert(t: T) = connection.query(insertQuery, relation.allParameters(t)).map(_ => ())
        def delete(t: T) = connection.query(deleteQuery, relation.keyParametersFromValue(t)).map(_ => ())
        def update(orig: T, value: T): F[Boolean] = if (orig == value) Monad[F].pure(false) else {
          relation.diff(orig, value).map {
            case (diffs, keyParams) =>
              connection.query(UpdateQuery(relation.tableName, diffs.map(_.name), relation.keyColumns), diffs.map(_.qp) ++ keyParams) map(_ => true)
          } valueOr { deleteParams =>
            connection.query(deleteQuery, deleteParams).flatMap(_ => insert(value)).map(_ => true)
          }
        }
        WriteQueries[T](insert, update, delete)
      }
      queries.reduce {
        (a, b) => WriteQueries(t => a.insert(t).flatMap(_ => b.insert(t)),
          (t1, t2) => a.update(t1, t2).flatMap(changed => b.update(t1, t2).map(_||changed)),
          t => a.delete(t).flatMap(_ => b.delete(t)))
      }
    }
  }

  class RelationPartial[T] {

    case class RelationBuilderPartial[T, Columns <: HList, ColumnsValues <: HList]
    (baseName: String, mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]) extends SingletonProductArgs {
      def key[K](k: Witness)(implicit ev: Selector[Columns, k.T]): RelationBuilder[T, Columns, ColumnsValues, k.T :: HNil] =
        RelationBuilder[T, Columns, ColumnsValues, k.T :: HNil](baseName, mapper)

      def keys(k1: Witness, k2: Witness)
              (implicit
               ev: SelectAllRecord[Columns, k1.T :: k2.T :: HNil]): RelationBuilder[T, Columns, ColumnsValues, k1.T :: k2.T :: HNil]
      = RelationBuilder[T, Columns, ColumnsValues, k1.T :: k2.T :: HNil](baseName, mapper)
    }


    def apply[Columns <: HList, ColumnsValues <: HList](name: String)(implicit gen: GenericColumnMapper.Aux[T, Columns, ColumnsValues])
    : RelationBuilderPartial[T, Columns, ColumnsValues] = RelationBuilderPartial(name, gen())
  }

  def relation[T] = new RelationPartial[T]

}
