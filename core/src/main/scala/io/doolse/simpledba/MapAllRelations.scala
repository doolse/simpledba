package io.doolse.simpledba

import shapeless.labelled._
import shapeless.ops.hlist.{Reverse, RightFolder}
import shapeless.ops.record.SelectAll
import shapeless.{DepFn1, HList, HNil, Poly2, Witness}

/**
  * Created by jolz on 8/06/16.
  */
trait MapAllRelations[In] extends DepFn1[In]

trait MapAllContext[E <: HList, R <: HList, CA[_]] {
  def ctx: ColumnMapperContext[CA, E]
  def relations: R
  def changeEmbedding[E2 <: HList](f: E => E2) = MapAllContext(ctx.copy(embeddedMappings = f(ctx.embeddedMappings)), relations)
}

object MapAllContext {
  def apply[E <: HList, R <: HList, CA[_]](_ctx: ColumnMapperContext[CA, E], _relations: R) = new MapAllContext[E, R, CA] {
    def ctx = _ctx
    def relations = _relations
  }
}

object MapAllRelations {
  type Aux[In, Out0] = MapAllRelations[In] {type Out = Out0}

  private object columnMapAll extends Poly2 {

    implicit def customAtom[S, A, E <: HList, R <: HList, CA[_]] = at[CustomAtom[S, A], MapAllContext[E, R, CA]] {
      case (custom, mapContext) => mapContext.changeEmbedding(m => field[S](custom) :: m)
    }

    implicit def embed[A, E <: HList, R <: HList, C <: HList, CV <: HList, CA[_]](implicit gm: GenericMapping.Aux[A, CA, E, C, CV])
    = at[Embed[A], MapAllContext[E, R, CA]] {
      case (_, ctx) => MapAllContext(gm.embed(ctx.ctx), ctx.relations)
    }

    implicit def lookupRelation[A, K <: Symbol, Keys <: HList, E <: HList, R <: HList, CR <: HList, CVL <: HList, CTX, CA[_]]
    (implicit
     gm: GenericMapping.Aux[A, CA, E, CR, CVL], w: Witness.Aux[K],
     ev: SelectAll[CR, Keys])
    = at[Relation[K, A, Keys], MapAllContext[E, R, CA]] {
      case (_, context) => MapAllContext(context.ctx, field[K](RelationDef[A, CR, Keys, CVL](w.value.name, gm.lookup(context.ctx))) :: context.relations)
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
