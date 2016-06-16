package io.doolse.simpledba

import cats.Applicative
import fs2.util.Catchable
import shapeless.{HList, HNil, tag}
import shapeless.tag.@@

/**
  * Created by jolz on 8/06/16.
  */
class ModelVerifierContext[R <: HList, E <: HList, CA[_], F[_], DDL, KMT, Q <: HList, As[_[_]]]
(rm: RelationModel[R, Q, As], cmc: ColumnMapperContext[CA, E], _M: Applicative[F], _C: Catchable[F]) extends MapAllContext[E, R, CA]
  with BuilderContext[F, DDL, KMT, Q]
  with ConvertVerifierContext[F, As] {
  def ctx = cmc

  def relations = rm.relations

  val M = _M
  val queries = rm.queryList
  val C = _C
}

case class ModelVerifier[In](errors: In => (String, List[String]))

trait ModelVerifierLP2 {
  implicit def noMapping[CTX, R <: HList, As[_[_]], CA[_]]
  (implicit
   ev: CTX <:< MapAllContext[_, R, CA],
   verifyMapping: ColumnMapperVerifier[ColumnMapperVerifierContext[CA, HNil], R])
  = ModelVerifier[CTX](ctx => ("Relations could not be mapped", ModelVerifier.bugIfNoError(verifyMapping.errors)))
}

trait ModelVerifierLP extends ModelVerifierLP2 {
  implicit def mappedOnly[CTX, CA[_], R <: HList, KMT, RelDefs <: HList, E <: HList, Q <: HList, F[_], DDL]
  (implicit
   ev: CTX <:< MapAllContext[E, R, CA],
   mappedRelations: MapAllRelations.Aux[MapAllContext[E, R, CA], RelDefs],
   ev3: CTX <:< BuilderContext[F, DDL, KMT, Q],
   verifyQueries: QueryBuilderVerifier[(QueryBuilderVerifierContext[F, DDL, RelDefs, KMT], Q)])
  = ModelVerifier[CTX] { ctx =>
    val relDefs = mappedRelations(ctx)
    ("Relations OK, Queries could not be mapped to physical tables", ModelVerifier.bugIfNoError(
      verifyQueries.errors((QueryBuilderVerifierContext(mappedRelations(ctx)), ev3(ctx).queries))))
  }
}

object ModelVerifier extends ModelVerifierLP {

  def bugIfNoError(e: List[String]) = if (e.isEmpty) List("There appears to be a bug in the verifier") else e

  implicit def mappedAndBuilt[CTX, E <: HList, R <: HList, CA[_],
  RelDefs <: HList, F[_], DDL, KMT, Q, QOut <: HList, As[_[_]]]
  (implicit
   ev: CTX <:< MapAllContext[E, R, CA],
   ev2: CTX <:< BuilderContext[F, DDL, KMT, Q],
   ev3: CTX <:< ConvertVerifierContext[F, As],
   mappedRelations: MapAllRelations.Aux[MapAllContext[E, R, CA], RelDefs],
   convertAndBuild: ConvertAndBuild.Aux[(BuilderContext[F, DDL, KMT, Q], RelDefs), BuiltQueries.Aux[QOut, DDL]],
   verifyConversion: ConvertVerifier[QOut, ConvertVerifierContext[F, As]])
  = ModelVerifier[CTX] { ctx =>
    val relDefs = mappedRelations(ctx)
    val errs = verifyConversion.errors(convertAndBuild((ctx, relDefs)).queries)
    (s"Relations OK, Queries OK, ${if (errs.isEmpty) "Conversion OK" else "Conversion failed"}", errs)
  }

}
