package io.doolse.simpledba

import shapeless.labelled._
import shapeless._
import shapeless.ops.adjoin.Adjoin
import shapeless.ops.hlist.{Comapped, Mapper, Prepend, ZipConst}
import shapeless.ops.record.{Keys, SelectAll, Selector, Values}

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

/**
  * Created by jolz on 10/05/16.
  */
class Mapper2[F[_], RSOps[_], PhysCol[_]](val connection: RelationIO.Aux[F, RSOps, PhysCol]) {
  type QueryParam = connection.QP

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

  trait ColumnMapping[A] {
    type S

    def columnName: ColumnName

    def atom: ColumnAtom[A]

    def get(t: S): A

    override def toString = s"('${columnName.name}',${atom})"
  }

  object ColumnMapping {
    type Aux[A, S0] = ColumnMapping[A] {
      type S = S0
    }
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] extends DepFn0 {
    type Out <: HList
  }

  object ColumnMapper {
    type Aux[A, Out0 <: HList] = ColumnMapper[A] {
      type Out = Out0
    }

    implicit def hnilMapper[TopLevel] = new ColumnMapper[(TopLevel, HNil)] {
      type Out = HNil

      def apply(): Out = HNil
    }

    implicit def singleColumnMapper[K <: Symbol, H, T <: HList, S0 <: HList]
    (implicit colName: Witness.Aux[K], headAtom: ColumnAtom[H], tailMapper: ColumnMapper[(S0, T)],
     select: MkRecordSelectLens.Aux[S0, K, H]) =
      new ColumnMapper[(S0, FieldType[K, H] :: T)] {
        type Out = FieldType[K, ColumnMapping.Aux[H, S0]] :: tailMapper.Out

        def apply(): Out = {
          val headField = field[K](new ColumnMapping[H] {
            type S = S0

            def columnName: ColumnName = ColumnName(colName.value.name)

            def atom: ColumnAtom[H] = headAtom

            def get(t: S): H = select().get(t)
          })
          headField :: tailMapper()
        }
      }

    implicit def genericMapper[K <: Symbol, H, T <: HList, HRepr, S <: HList,
    SubColumns <: HList, SubColumnsZipped <: HList, SubColumnsLensed <: HList, TailOut <: HList]
    (implicit headRecord: LabelledGeneric.Aux[H, HRepr],
     tailMapper: ColumnMapper.Aux[(S, T), TailOut],
     select: MkRecordSelectLens.Aux[S, K, H],
     childMappings: ColumnMapper.Aux[(HRepr, HRepr), SubColumns],
     zippedWithLens: ZipConst.Aux[S => HRepr, SubColumns, SubColumnsZipped],
     lensed: Mapper.Aux[composeSubColumns.type, SubColumnsZipped, SubColumnsLensed],
     prepend: Prepend[SubColumnsLensed, TailOut]
    ): ColumnMapper.Aux[(S, FieldType[K, H] :: T), prepend.Out] =
      new ColumnMapper[(S, FieldType[K, H] :: T)] {
        type Out = prepend.Out

        def apply() = prepend(lensed(zippedWithLens(select().get _ andThen headRecord.to, childMappings())), tailMapper())
      }

    object composeSubColumns extends Poly1 {
      implicit def mapToTopLevel[K, S2, A, S0] = at[(FieldType[K, ColumnMapping.Aux[A, S2]], S0 => S2)] {
        case (mapping, lens) => field[K](new ColumnMapping[A] {
          type S = S0

          def columnName: ColumnName = mapping.columnName

          def atom: ColumnAtom[A] = mapping.atom

          def get(t: S): A = mapping.get(lens(t))
        })
      }
    }

  }

  case class Relation[T, Repr <: HList, Columns <: HList, Keys <: HList](baseName: String, lgen: LabelledGeneric.Aux[T, Repr],
                                                                         mapper: ColumnMapper.Aux[(Repr, Repr), Columns], keys: Keys) {
    def key(k: Witness)(implicit ev: Selector[Columns, k.T]) = copy[T, Repr, Columns, k.T :: Keys](keys = k.value :: keys)

    def compositeKey(k1: Witness, k2: Witness)(implicit ev: SelectAll[Columns, k1.T :: k2.T :: HNil]) = copy[T, Repr, Columns, k1.T :: k2.T :: Keys](keys = k1.value :: k2.value :: keys)

    def queryByPK[Sel <: HList](implicit allColumns: SelectAll.Aux[Columns, Keys, Sel]) = ???
  }

  class RelationPartial[T] {
    def apply[Repr <: HList, Columns <: HList](name: String)(implicit lgen: LabelledGeneric.Aux[T, Repr],
                                                             mapper: ColumnMapper.Aux[(Repr, Repr), Columns]): Relation[T, Repr, Columns, HNil] = Relation(name, lgen, mapper, HNil)
  }

  def relation[T] = new RelationPartial[T]

}
