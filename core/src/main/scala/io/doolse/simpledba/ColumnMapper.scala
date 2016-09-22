package io.doolse.simpledba

import shapeless.PolyDefns.identity
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist.{Drop, Length, Prepend, Take}
import shapeless.ops.record.Selector

/**
  * Created by jolz on 27/05/16.
  */

trait ColumnComposer[S, S2] {
  def apply[CA[_], A](cm: ColumnMapping[CA, S, A]): ColumnMapping[CA, S2, A]
}

/**
  * For some reason this wouldn't work with a Poly2 because of
  * divergent implicit expansion - hmmm maybe use Lazy?
  */
trait ColumnsComposed[In, S1, S2] extends DepFn2[In, ColumnComposer[S1, S2]] {
  type Out
}

object ColumnsComposed {

  trait Aux[In, S1, S2, Out0] extends ColumnsComposed[In, S1, S2] {
    type Out = Out0
  }

  implicit def fieldCompose[CA[_], S1, S2, K, A] = new Aux[FieldType[K, ColumnMapping[CA, S1, A]], S1, S2, FieldType[K, ColumnMapping[CA, S2, A]]] {
    def apply(t: FieldType[K, ColumnMapping[CA, S1, A]], u: ColumnComposer[S1, S2]): FieldType[K, ColumnMapping[CA, S2, A]] = field[K](u(t))
  }

  implicit def hnilCompose[S1, S2] = new Aux[HNil, S1, S2, HNil] {
    def apply(t: HNil, u: ColumnComposer[S1, S2]) = t
  }

  implicit def hconsCompose[CA[_], S1, S2, H, T <: HList, TOut <: HList]
  (implicit h: ColumnsComposed[H, S1, S2], t: ColumnsComposed.Aux[T, S1, S2, TOut]
  ) = new Aux[H :: T, S1, S2, h.Out :: t.Out] {
    def apply(a: H :: T, u: ColumnComposer[S1, S2]) = h(a.head, u) :: t(a.tail, u)
  }
}

trait MappingCreator[ColumnAtom[_]] {
  def wrapAtom[S, A](atom: ColumnAtom[A], customAtom: CustomAtom[S, A]): ColumnAtom[S]

  def makeMapping[S, A](name: String, atom: ColumnAtom[A], get: S => A) = ColumnMapping(name, atom, get)

  def composer[S, S2](f: S2 => S): ColumnComposer[S, S2] = new ColumnComposer[S, S2] {
    def apply[CA[_], A](cm: ColumnMapping[CA, S, A]): ColumnMapping[CA, S2, A] = cm.copy(get = cm.get compose f)
  }
}

case class ColumnMapping[CA[_], S, A](name: String, atom: CA[A], get: S => A)

case class ColumnMapper[A, Columns <: HList, ColumnsValues <: HList](columns: Columns, fromColumns: ColumnsValues => A,
                                                                     toColumns: A => ColumnsValues)

case class ColumnMapperContext[CA[_], E <: HList](ops: MappingCreator[CA], embeddedMappings: E = HList())


trait ColumnMapperBuilder[A, C] extends DepFn1[C]

trait ColumnMapperBuilderLP {
  implicit def multiColumn[CA[_], E <: HList, K <: Symbol, V,
  VCM, C0 <: HList, CV0 <: HList, CZ <: HList, COut <: HList]
  (implicit
   selectMapping: Selector.Aux[E, V, VCM],
   ev: VCM <:< ColumnMapper[V, C0, CV0],
   composer: ColumnsComposed.Aux[C0, V, FieldType[K, V], COut]
  )
  = ColumnMapperBuilder.mapper[FieldType[K, V], ColumnMapperContext[CA, E], COut, CV0] { t =>
    val otherMapper = ev(selectMapping(t.embeddedMappings))
    new ColumnMapper(composer(otherMapper.columns, t.ops.composer(identity)),
      cv => field[K](otherMapper.fromColumns(cv)),
      fld => otherMapper.toColumns(fld: V))
  }

  implicit def singleIsoColumn[CA[_], E <: HList, K <: Symbol, V, A,
  VCM, C0 <: HList, CV0 <: HList, CZ <: HList, COut <: HList]
  (implicit
   selectMapping: Selector.Aux[E, V, VCM],
   cname: Witness.Aux[K],
   ev: VCM <:< CustomAtom[V, A],
   atom: CA[A]
  ) = ColumnMapperBuilder.mapper[FieldType[K, V], ColumnMapperContext[CA, E], FieldType[K, ColumnMapping[CA, FieldType[K, V], V]] :: HNil, V :: HNil] { t =>
    val ca = ev(selectMapping(t.embeddedMappings))
    new ColumnMapper(field[K](t.ops.makeMapping(cname.value.name, t.ops.wrapAtom(atom, ca), (f: FieldType[K, V]) => f: V)) :: HNil,
      cv => field[K](cv.head),
      fld => (fld: V) :: HNil)
  }
}

object ColumnMapperBuilder extends ColumnMapperBuilderLP {

  type Aux[A, C, Columns <: HList, ColumnsValues <: HList] = ColumnMapperBuilder[A, C] {
    type Out = ColumnMapper[A, Columns, ColumnsValues]
  }

  def mapper[A, C, Columns <: HList, ColumnsValues <: HList]
  (cm: C => ColumnMapper[A, Columns, ColumnsValues]): Aux[A, C, Columns, ColumnsValues] = new ColumnMapperBuilder[A, C] {
    type Out = ColumnMapper[A, Columns, ColumnsValues]

    def apply(t: C) = cm(t)
  }

  implicit def hnilMapper[C, L <: HNil] = mapper[L, C, HNil, HNil](_ => new ColumnMapper(HNil, v => v.asInstanceOf[L], identity))

  implicit def singleColumn[CA[_], E <: HList, K <: Symbol, V]
  (implicit atom: CA[V], key: Witness.Aux[K]) =
    mapper[FieldType[K, V], ColumnMapperContext[CA, E], FieldType[K, ColumnMapping[CA, FieldType[K, V], V]] :: HNil, V :: HNil] { c =>
      new ColumnMapper(field[K](c.ops.makeMapping(key.value.name, atom,
        (f: FieldType[K, V]) => f: V)) :: HNil,
        v => field[K](v.head),
        fv => (fv: V) :: HNil)
    }


  implicit def hconsMapper[C, CA[_], CM[_, _],
  H, T <: HList,
  HC <: HList, HCZ <: HList, HCM <: HList,
  TC <: HList, TCZ <: HList, TCM <: HList,
  HV <: HList, TV <: HList,
  OutV <: HList, LenHV <: Nat]
  (implicit
   ev: C <:< ColumnMapperContext[CA, _],
   headMapper: ColumnMapperBuilder.Aux[H, C, HC, HV], tailMapper: ColumnMapperBuilder.Aux[T, C, TC, TV],
   headComposer: ColumnsComposed.Aux[HC, H, H :: T, HCM], tailComposer: ColumnsComposed.Aux[TC, T, H :: T, TCM],
   prependC: Prepend[HCM, TCM], prependV: Prepend.Aux[HV, TV, OutV],
   lenH: Length.Aux[HV, LenHV], headVals: Take.Aux[OutV, LenHV, HV], tailVals: Drop.Aux[OutV, LenHV, TV]) =
    mapper[H :: T, C, prependC.Out, OutV] { c =>
      val ops = ev(c).ops
      val hm = headMapper(c)
      val tm = tailMapper(c)
      new ColumnMapper(
        prependC(headComposer(hm.columns, ops.composer((_: H :: T).head)),
          tailComposer(tm.columns, ops.composer((_: H :: T).tail))),
        v => hm.fromColumns(headVals(v)) :: tm.fromColumns(tailVals(v)),
        v => prependV(hm.toColumns(v.head), tm.toColumns(v.tail)))
    }

}

trait GenericMapping[A, CA[_], E <: HList] {
  type C <: HList
  type CV <: HList

  def lookup(context: ColumnMapperContext[CA, E]): ColumnMapper[A, C, CV]

  def embed(context: ColumnMapperContext[CA, E]): ColumnMapperContext[CA, FieldType[A, ColumnMapper[A, C, CV]] :: E]
}

object GenericMapping {

  trait Aux[A, CA[_], E <: HList, C0 <: HList, CV0 <: HList] extends GenericMapping[A, CA, E] {
    type C = C0
    type CV = CV0
  }

  implicit def genMapping[A, CA[_], E <: HList, Repr, C0 <: HList, CV0 <: HList, CC <: HList]
  (implicit lgen: LabelledGeneric.Aux[A, Repr],
   b: ColumnMapperBuilder.Aux[Repr, ColumnMapperContext[CA, E], C0, CV0],
   compose: ColumnsComposed.Aux[C0, Repr, A, CC]) = new Aux[A, CA, E, CC, CV0] {

    def lookup(context: ColumnMapperContext[CA, E]) = {
      val recMapping = b(context)
      new ColumnMapper[A, CC, CV](compose(recMapping.columns, context.ops.composer(lgen.to)),
        v => lgen.from(recMapping.fromColumns(v)), a => recMapping.toColumns(lgen.to(a)))
    }

    def embed(context: ColumnMapperContext[CA, E]) =
      context.copy(embeddedMappings = field[A](lookup(context)) :: context.embeddedMappings)
  }
}
