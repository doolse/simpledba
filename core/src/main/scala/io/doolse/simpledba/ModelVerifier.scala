package io.doolse.simpledba

import cats.Applicative
import fs2.util.Catchable
import shapeless.ops.hlist.Mapper
import shapeless.{HList, HNil}
import shapeless.poly.Case3
/**
  * Created by jolz on 8/06/16.
  */

trait KeyMapperContext[Q, KeyMapperPoly, QBPoly, MapperConfig] {
  def queries : Q
  def config : MapperConfig
}
class ModelVerifierContext[R <: HList, E <: HList, CA[_], F[_], DDL, KeyMapperPoly, QBPoly, Q <: HList, As[_[_]], MapperConfig]
(rm: RelationModel[R, Q, As], cmc: ColumnMapperContext[CA, E], val config: MapperConfig)
  extends MapAllContext[E, R, CA] with KeyMapperContext[Q, KeyMapperPoly, QBPoly, MapperConfig]
  with ConvertVerifierContext[F, As] {
  def ctx = cmc
  def relations = rm.relations
  val queries = rm.queryList
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
  implicit def mappedOnly[CTX, CA[_], R <: HList, KMT, RelDefs <: HList, E <: HList, Q <: HList]
  (implicit
   ev: CTX <:< MapAllContext[E, R, CA],
   ev2: CTX <:< KeyMapperContext[Q, KMT, _, _],
   mappedRelations: MapAllRelations.Aux[MapAllContext[E, R, CA], RelDefs],
   verifyQueries: QueryBuilderVerifier[(QueryBuilderVerifierContext[RelDefs, KMT], Q)]
   )
  = ModelVerifier[CTX] { ctx =>
    val relDefs = mappedRelations(ctx)
    ("Relations OK, Queries could not be mapped to physical tables", ModelVerifier.bugIfNoError(
      verifyQueries.errors((QueryBuilderVerifierContext(mappedRelations(ctx)), ev2(ctx).queries))))
  }
}

object ModelVerifier extends ModelVerifierLP {

  def bugIfNoError(e: List[String]) = if (e.isEmpty) List("There appears to be a bug in the verifier") else e

  implicit def mappedAndBuilt[CTX, E <: HList, R <: HList, CA[_],
  RelDefs <: HList, F[_], DDL, KeyMapperPoly, QueriesPoly, Q <: HList, QOut <: HList, As[_[_]],
  RelWithQ <: HList, MappedTables <: HList, MapperConfig]
  (implicit
   ev: CTX <:< MapAllContext[E, R, CA],
   ev2: CTX <:< KeyMapperContext[Q, KeyMapperPoly, QueriesPoly, MapperConfig],
   ev3: CTX <:< ConvertVerifierContext[F, As],
   mappedRelations: MapAllRelations.Aux[MapAllContext[E, R, CA], RelDefs],
   mapWith: MapWith.Aux[RelDefs, Q, zipWithRelation.type, RelWithQ],
   tableMap: Mapper.Aux[KeyMapperPoly, RelWithQ, MappedTables],
   buildQueries: Case3.Aux[QueriesPoly, RelWithQ, MappedTables, MapperConfig, BuiltQueries.Aux[QOut, DDL]],
   verifyConversion: ConvertVerifier[QOut, ConvertVerifierContext[F, As]])
  = ModelVerifier[CTX] { ctx =>
    val relDefs = mappedRelations(ctx)
    val mapperContext = ev2(ctx)
    val relWithQ = mapWith(relDefs, mapperContext.queries)
    val errs = verifyConversion.errors(buildQueries(HList(relWithQ, tableMap(relWithQ), mapperContext.config)).queries)
    (s"Relations OK, Queries OK, ${if (errs.isEmpty) "Conversion OK" else "Conversion failed"}", errs)
  }

}
