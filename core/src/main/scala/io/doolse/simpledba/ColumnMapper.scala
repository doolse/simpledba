package io.doolse.simpledba

import shapeless.PolyDefns.identity
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist.{Drop, Length, Prepend, Take}
import shapeless.ops.record.Selector

/**
  * Created by jolz on 27/05/16.
  */

trait ColumnComposer[CM[_, _], S, S2] {
  def apply[A](cm: CM[S, A]): CM[S2, A]
}

/**
  * For some reason this wouldn't work with a Poly2 because of
  * divergent implicit expansion - hmmm maybe use Lazy?
  */
trait ColumnsComposed[In, CM[_, _], S1, S2] extends DepFn2[In, ColumnComposer[CM, S1, S2]] {
  type Out
}

object ColumnsComposed {

  trait Aux[In, CM[_, _], S1, S2, Out0] extends ColumnsComposed[In, CM, S1, S2] {
    type Out = Out0
  }

  implicit def fieldCompose[CM[_, _], S1, S2, K, A] = new Aux[FieldType[K, CM[S1, A]], CM, S1, S2, FieldType[K, CM[S2, A]]] {
    def apply(t: FieldType[K, CM[S1, A]], u: ColumnComposer[CM, S1, S2]): FieldType[K, CM[S2, A]] = field[K](u(t))
  }

  implicit def hnilCompose[CM[_, _], S1, S2] = new Aux[HNil, CM, S1, S2, HNil] {
    def apply(t: HNil, u: ColumnComposer[CM, S1, S2]) = t
  }

  implicit def hconsCompose[CM[_, _], S1, S2, H, T <: HList, TOut <: HList]
  (implicit h: ColumnsComposed[H, CM, S1, S2], t: ColumnsComposed.Aux[T, CM, S1, S2, TOut]
  ) = new Aux[H :: T, CM, S1, S2, h.Out :: t.Out] {
    def apply(a: H :: T, u: ColumnComposer[CM, S1, S2]) = h(a.head, u) :: t(a.tail, u)
  }
}

trait MappingCreator[ColumnAtom[_], ColumnMapping[_, _]] {
  def wrapAtom[S, A](atom: ColumnAtom[A], to: S => A, from: A => S): ColumnAtom[S]

  def makeMapping[S, A](name: String, atom: ColumnAtom[A], get: S => A): ColumnMapping[S, A]

  def composer[S, S2](f: S2 => S): ColumnComposer[ColumnMapping, S, S2]
}

case class ColumnMapper[A, Columns <: HList, ColumnsValues <: HList](columns: Columns, fromColumns: ColumnsValues => A,
                                                                     toColumns: A => ColumnsValues)

case class ColumnMapperContext[CA[_], CM[_, _], E <: HList](ops: MappingCreator[CA, CM], embeddedMappings: E = HList())


trait ColumnMapperBuilder[A, C] extends DepFn1[C]

trait ColumnMapperBuilderLP {
  implicit def multiColumn[CA[_], CM[_, _], E <: HList, K <: Symbol, V,
  VCM, C0 <: HList, CV0 <: HList, CZ <: HList, COut <: HList]
  (implicit
   selectMapping: Selector.Aux[E, V, VCM],
   ev: VCM <:< ColumnMapper[V, C0, CV0],
   composer: ColumnsComposed.Aux[C0, CM, V, FieldType[K, V], COut]
  )
  = new ColumnMapperBuilder.Aux[FieldType[K, V], CA, CM, E, COut, CV0] {
    def apply(t: ColumnMapperContext[CA, CM, E]) = {
      val otherMapper = ev(selectMapping(t.embeddedMappings))
      new ColumnMapper(composer(otherMapper.columns, t.ops.composer(identity)),
        cv => field[K](otherMapper.fromColumns(cv)),
        fld => otherMapper.toColumns(fld: V))
    }
  }

  implicit def singleIsoColumn[CA[_], CM[_, _], E <: HList, K <: Symbol, V, A,
  VCM, C0 <: HList, CV0 <: HList, CZ <: HList, COut <: HList]
  (implicit
   selectMapping: Selector.Aux[E, V, VCM],
   ev: VCM <:< (V => A, A => V),
   atom: CA[A]
  ) = new ColumnMapperBuilder.Aux[FieldType[K, V], CA, CM, E, FieldType[K, CM[FieldType[K, V], V]] :: HNil, V :: HNil] {
    def apply(t: ColumnMapperContext[CA, CM, E]) = ???
  }
}

object ColumnMapperBuilder extends ColumnMapperBuilderLP {

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


  implicit def hconsMapper[CA[_], CM[_, _], E <: HList,
  H, T <: HList,
  HC <: HList, HCZ <: HList, HCM <: HList,
  TC <: HList, TCZ <: HList, TCM <: HList,
  HV <: HList, TV <: HList,
  OutV <: HList, LenHV <: Nat]
  (implicit headMapper: ColumnMapperBuilder.Aux[H, CA, CM, E, HC, HV], tailMapper: ColumnMapperBuilder.Aux[T, CA, CM, E, TC, TV],
   headComposer: ColumnsComposed.Aux[HC, CM, H, H :: T, HCM], tailComposer: ColumnsComposed.Aux[TC, CM, T, H :: T, TCM],
   prependC: Prepend[HCM, TCM], prependV: Prepend.Aux[HV, TV, OutV],
   lenH: Length.Aux[HV, LenHV], headVals: Take.Aux[OutV, LenHV, HV], tailVals: Drop.Aux[OutV, LenHV, TV]) =
    new Aux[H :: T, CA, CM, E, prependC.Out, OutV] {
      def apply(c: ColumnMapperContext[CA, CM, E]) = {
        val hm = headMapper(c)
        val tm = tailMapper(c)
        new ColumnMapper(
          prependC(headComposer(hm.columns, c.ops.composer((_: H :: T).head)),
            tailComposer(tm.columns, c.ops.composer((_: H :: T).tail))),
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

  trait Aux[A, CA[_], CM[_, _], E <: HList, C0 <: HList, CV0 <: HList] extends GenericMapping[A, CA, CM, E] {
    type C = C0
    type CV = CV0
  }

  implicit def genMapping[A, CA[_], CM[_, _], E <: HList, Repr, C0 <: HList, CV0 <: HList, CC <: HList]
  (implicit lgen: LabelledGeneric.Aux[A, Repr],
   b: ColumnMapperBuilder.Aux[Repr, CA, CM, E, C0, CV0],
   compose: ColumnsComposed.Aux[C0, CM, Repr, A, CC]) = new Aux[A, CA, CM, E, CC, CV0] {

    def lookup(context: ColumnMapperContext[CA, CM, E]) = {
      val recMapping = b(context)
      new ColumnMapper[A, CC, CV](compose(recMapping.columns, context.ops.composer(lgen.to)),
        v => lgen.from(recMapping.fromColumns(v)), a => recMapping.toColumns(lgen.to(a)))
    }

    def embed(context: ColumnMapperContext[CA, CM, E]) =
      context.copy(embeddedMappings = field[A](lookup(context)) :: context.embeddedMappings)
  }
}
