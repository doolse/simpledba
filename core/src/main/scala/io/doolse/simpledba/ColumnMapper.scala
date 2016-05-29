package io.doolse.simpledba

import shapeless.PolyDefns.identity
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist.{Drop, Length, Mapper, Prepend, Take, ZipConst}
import shapeless.ops.record.{Selector, Updater}
import shapeless.record._
import shapeless.syntax.singleton._

/**
  * Created by jolz on 27/05/16.
  */

trait ColumnComposer[CM[_, _], S, S2] {
  def apply[A](cm: CM[S, A]): CM[S2, A]
}

object composeLens extends Poly1 {
  implicit def convertLens[CM[_, _], T, T2, K, A]
  = at[(FieldType[K, CM[T, A]], ColumnComposer[CM, T, T2])] {
    case (colMapping, c) => field[K](c(colMapping: CM[T, A]))
  }
}

trait MappingCreator[ColumnAtom[_], ColumnMapping[_, _]] {
  def makeMapping[S, A](name: String, atom: ColumnAtom[A], get: S => A): ColumnMapping[S, A]

  def composer[S, S2](f: S2 => S): ColumnComposer[ColumnMapping, S, S2]
}

case class ColumnMapper[A, Columns <: HList, ColumnsValues <: HList](columns: Columns, fromColumns: ColumnsValues => A,
                                                                     toColumns: A => ColumnsValues)

case class ColumnMapperContext[CA[_], CM[_, _], E <: HList](ops: MappingCreator[CA, CM], embeddedMappings: E = HList())

object ColumnMapperContext {

  implicit class ColumnMapperOps[CA[_], CM[_, _], E <: HList](context: ColumnMapperContext[CA, CM, E]) {

    def lookup[A](implicit gm: GenericMapping[A, CA, CM, E]) = gm.lookup(context)

    def embed[A](implicit gm: GenericMapping[A, CA, CM, E]) = gm.embed(context)
  }

}

trait ColumnMapperBuilder[A, C] extends DepFn1[C]


object ColumnMapperBuilder {


  trait Aux[A, CA[_], CM[_, _], E <: HList, Columns <: HList, ColumnsValues <: HList]
    extends ColumnMapperBuilder[A, ColumnMapperContext[CA, CM, E]] {
    type Out = ColumnMapper[A, Columns, ColumnsValues]
  }

  implicit def hnilMapper[CA[_], CM[_, _], E <: HList] = new Aux[HNil, CA, CM, E, HNil, HNil] {
    def apply(c: ColumnMapperContext[CA, CM, E]) = new ColumnMapper(HNil: HNil, identity, identity)
  }

  implicit def singleColumn[CA[_], CM[_, _], E <: HList, K <: Symbol, V]
  (implicit atom: CA[V], key: Witness.Aux[K]) =
    new Aux[FieldType[K, V], CA, CM, E, FieldType[K, CM[FieldType[K, V], V]] :: HNil, V :: HNil] {
      def apply(c: ColumnMapperContext[CA, CM, E]) = new ColumnMapper(field[K](c.ops.makeMapping(key.value.name, atom,
        (f: FieldType[K, V]) => f: V)) :: HNil,
        v => field[K](v.head),
        fv => (fv: V) :: HNil)
    }

  implicit def multiColumn[CA[_], CM[_, _], E <: HList, K <: Symbol, V,
  VCM, C0 <: HList, CV0 <: HList, CZ <: HList]
  (implicit
   selectMapping: Selector.Aux[E, V, VCM],
   ev: ColumnMapper[V, C0, CV0] =:= VCM,
   ev2: VCM =:= ColumnMapper[V, C0, CV0],
   zipWithLens: ZipConst.Aux[ColumnComposer[CM, V, FieldType[K, V]], C0, CZ],
   mapper: Mapper[composeLens.type, CZ]
  )
  = new Aux[FieldType[K, V], CA, CM, E, mapper.Out, CV0] {
    def apply(t: ColumnMapperContext[CA, CM, E]) = {
      val otherMapper = ev2(selectMapping(t.embeddedMappings))
      new ColumnMapper(mapper(zipWithLens(t.ops.composer(fv => fv:V), otherMapper.columns)),
        cv => field[K](otherMapper.fromColumns(cv)),
        fld => otherMapper.toColumns(fld:V))
    }
  }

  implicit def hconsMapper[CA[_], CM[_, _], E <: HList,
  H, T <: HList,
  HC <: HList, HCZ <: HList, HCM <: HList,
  TC <: HList, TCZ <: HList, TCM <: HList,
  HV <: HList, TV <: HList,
  OutV <: HList, LenHV <: Nat]
  (implicit headMapper: ColumnMapperBuilder.Aux[H, CA, CM, E, HC, HV], tailMapper: ColumnMapperBuilder.Aux[T, CA, CM, E, TC, TV],
   zipHeadLens: ZipConst.Aux[ColumnComposer[CM, H, H :: T], HC, HCZ], zipTailLens: ZipConst.Aux[ColumnComposer[CM, T, H :: T], TC, TCZ],
   mappedHead: Mapper.Aux[composeLens.type, HCZ, HCM], mappedTail: Mapper.Aux[composeLens.type, TCZ, TCM],
   prependC: Prepend[HCM, TCM], prependV: Prepend.Aux[HV, TV, OutV],
   lenH: Length.Aux[HV, LenHV], headVals: Take.Aux[OutV, LenHV, HV], tailVals: Drop.Aux[OutV, LenHV, TV]) =
    new Aux[H :: T, CA, CM, E, prependC.Out, OutV] {
      def apply(c: ColumnMapperContext[CA, CM, E]) = {
        val hm = headMapper(c)
        val tm = tailMapper(c)
        new ColumnMapper(
          prependC(mappedHead(zipHeadLens(c.ops.composer((_: H :: T).head), hm.columns)), mappedTail(zipTailLens(c.ops.composer((_: H :: T).tail), tm.columns))),
          v => hm.fromColumns(headVals(v)) :: tm.fromColumns(tailVals(v)),
          v => prependV(hm.toColumns(v.head), tm.toColumns(v.tail)))
      }
    }
}

trait GenericMapping[A, CA[_], CM[_, _], E <: HList] {
  type C <: HList
  type CV <: HList

  def lookup(context: ColumnMapperContext[CA, CM, E]): ColumnMapper[A, C, CV]

  def embed(context: ColumnMapperContext[CA, CM, E]): ColumnMapperContext[CA, CM, FieldType[A, ColumnMapper[A, C, CV]] :: E]
}

object GenericMapping {
  implicit def genMapping[A, CA[_], CM[_, _], E <: HList, Repr, C0 <: HList, CV0 <: HList, CC <: HList, COut <: HList]
  (implicit lgen: LabelledGeneric.Aux[A, Repr],
   b: ColumnMapperBuilder.Aux[Repr, CA, CM, E, C0, CV0],
   zipWithLens: ZipConst.Aux[ColumnComposer[CM, Repr, A], C0, CC],
   compose: Mapper.Aux[composeLens.type, CC, COut]) = new GenericMapping[A, CA, CM, E] {
    type C = COut
    type CV = CV0

    def lookup(context: ColumnMapperContext[CA, CM, E]) = {
      val recMapping = b(context)
      new ColumnMapper[A, C, CV](compose(zipWithLens(context.ops.composer(lgen.to), recMapping.columns)),
        v => lgen.from(recMapping.fromColumns(v)), a => recMapping.toColumns(lgen.to(a)))
    }

    def embed(context: ColumnMapperContext[CA, CM, E]) =
      context.copy(embeddedMappings = field[A](lookup(context)) :: context.embeddedMappings)
  }
}
