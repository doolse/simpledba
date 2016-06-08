package io.doolse.simpledba

import shapeless.labelled._
import shapeless.ops.hlist.{Reverse, RightFolder}
import shapeless.{DepFn1, HList, HNil, Poly2, Witness}

/**
  * Created by jolz on 8/06/16.
  */
trait MapAllRelations[In] extends DepFn1[In]
case class MapAllContext[E <: HList, R <: HList, CA[_]](ctx: ColumnMapperContext[CA, E], relations: R)

object MapAllRelations {
  type Aux[In, Out0] = MapAllRelations[In] {type Out = Out0}

  private object columnMapAll extends Poly2 {

    implicit def customAtom[S, A, E <: HList, R <: HList, CA[_]] = at[CustomAtom[S, A], MapAllContext[E, R, CA]] {
      case (custom, MapAllContext(context, r)) => MapAllContext(context.copy(embeddedMappings = field[S](custom) :: context.embeddedMappings), r)
    }

    implicit def embed[A, E <: HList, R <: HList, C <: HList, CV <: HList, CA[_]](implicit gm: GenericMapping.Aux[A, CA, E, C, CV])
    = at[Embed[A], MapAllContext[E, R, CA]] {
      case (_, MapAllContext(context, r)) => MapAllContext(gm.embed(context), r)
    }

    implicit def lookupRelation[A, K <: Symbol, Keys <: HList, E <: HList, R <: HList, CR <: HList, CVL <: HList, CTX, CA[_]]
    (implicit
     gm: GenericMapping.Aux[A, CA, E, CR, CVL], w: Witness.Aux[K])
    = at[Relation[K, A, Keys], MapAllContext[E, R, CA]] {
      case (_, MapAllContext(context, r)) => MapAllContext(context, field[K](RelationDef[A, CR, Keys, CVL](w.value.name, gm.lookup(context))) :: r)
    }
  }


  implicit def mapAll[R <: HList, E <: HList, ER <: HList, CTXO, CRD <: HList, CA[_]]
  (implicit
   rev: Reverse.Aux[R, ER], // LeftFolder caused diverging implicits
   rf: RightFolder.Aux[ER, MapAllContext[E, R, CA], columnMapAll.type, CTXO],
   ev: CTXO <:< MapAllContext[_, CRD, CA]): Aux[MapAllContext[E, R, CA], CRD]
  = new MapAllRelations[MapAllContext[E, R, CA]] {
    type Out = CRD

    def apply(t: MapAllContext[E, R, CA]) = ev(rf(rev(t.relations), t)).relations
  }
}
