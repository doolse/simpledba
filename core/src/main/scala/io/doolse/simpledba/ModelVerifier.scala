package io.doolse.simpledba

import shapeless.{HList, HNil, tag}
import shapeless.tag.@@

/**
  * Created by jolz on 8/06/16.
  */

case class ModelVerifierContext[F[_], DDL, KMT, R <: HList, Q <: HList, As[_[_]], CA[_], CMC](rm: RelationModel[R, Q, As], context: CMC)
case class ModelVerifier[In](errors: In => List[String])

trait ModelVerifierLP2 {
  implicit def noMapping[F[_], DDL, R <: HList, Q <: HList, As[_[_]], CA[_], KMT, CMC]
  (implicit verifyMapping: ColumnMapperVerifier[VerifierContext[CA, HNil], R])
  = ModelVerifier[ModelVerifierContext[F, DDL, KMT, R, Q, As, CA, CMC]] { ctx => verifyMapping.errors }
}

trait ModelVerifierLP extends ModelVerifierLP2 {
  implicit def mappedOnly[F[_], DDL, R <: HList, Q <: HList, As[_[_]], CA[_], KMT, CMC, RelDefs <: HList]
  (implicit mappedRelations: MapAllRelations.Aux[(R, CMC), RelDefs],
   verifyQueries: QueryBuilderVerifier[(QueryBuilderVerifierContext[RelDefs, KMT], Q)])
  = ModelVerifier[ModelVerifierContext[F, DDL, KMT, R, Q, As, CA, CMC]] { ctx =>
    val relDefs = mappedRelations(ctx.rm.relations, ctx.context)
    verifyQueries.errors(QueryBuilderVerifierContext[RelDefs, KMT](relDefs), ctx.rm.queryList)
  }
}

object ModelVerifier extends ModelVerifierLP {
  implicit def mappedAndBuilt[F[_], DDL, KMT, R <: HList, Q <: HList, As[_[_]], CA[_], CMC, RelDefs <: HList, QOut <: HList]
  (implicit mappedRelations: MapAllRelations.Aux[CMC, RelDefs],
   convertAndBuild: ConvertAndBuild.Aux[(RelDefs, Q), F, DDL, KMT, QOut],
   verifyConversion: ConvertVerifier[QOut, ConvertVerifierContext[F, As]])
  = ModelVerifier[ModelVerifierContext[F, DDL, KMT, R, Q, As, CA, CMC]] { ctx =>
    val relDefs = mappedRelations(ctx.context)
    verifyConversion.errors(convertAndBuild((relDefs, ctx.rm.queryList)).queries)
  }

}
