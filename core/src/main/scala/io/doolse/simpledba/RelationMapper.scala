package io.doolse.simpledba

import cats.{Applicative, Eval, Monad}
import fs2.util.Catchable
import shapeless._
import shapeless.ops.hlist.Mapper
import shapeless.ops.record.Selector
import shapeless.tag.@@
import poly._

/**
  * Created by jolz on 10/05/16.
  */

abstract class RelationMapper[F[_]] {
  def M: Applicative[F]
  def C: Catchable[F]
  type DDLStatement
  type KeyMapperT
  type ColumnAtom[A]
  type MapperConfig
  type KeyMapperPoly <: Poly1
  type QueriesPoly <: Poly3

  val config : MapperConfig

  def stdColumnMaker : MappingCreator[ColumnAtom]

  def buildModel[R <: HList, Q <: HList, CRD <: HList, RDQ <: HList,
  QL <: HList, QOut <: HList, As[_[_]], AsRepr <: HList, QOutTag <: HList]
  (rm: RelationModel[R, Q, As])
  (implicit
   mapRelations: MapAllRelations.Aux[MapAllContext[HNil, R, ColumnAtom], CRD],
   convertAndBuild: ConvertAndBuild.Aux[(BuilderContext[F, DDLStatement, KeyMapperT, Q], CRD), BuiltQueries.Aux[QOut, DDLStatement]],
   genAs: Generic.Aux[As[F], AsRepr],
   zip: ZipWithTag.Aux[QOut, AsRepr, QOutTag],
   convert: Mapper.Aux[queriesAs.type, QOutTag, AsRepr]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {
    val relations = mapRelations(MapAllContext(ColumnMapperContext(stdColumnMaker, HNil), rm.relations))
    val rawQueries = convertAndBuild(BuilderContext(M, C, rm.queryList), relations)
    BuiltQueries(genAs.from(convert(zip(rawQueries.queries))), Eval.later(rawQueries.ddl))
  }

  def verifyModel[R <: HList, Q <: HList, C2, As[_[_]]]
  (rm: RelationModel[R, Q, As], p: String => Unit = Console.err.println)
  (implicit
   verify: ModelVerifier[ModelVerifierContext[R, HNil, ColumnAtom, F, DDLStatement, KeyMapperT, Q, As]]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {

    val (name, errors) = verify.errors(
      new ModelVerifierContext(rm, ColumnMapperContext(stdColumnMaker), M, C)
    )
    p(name)
    errors.foreach(p)
    new BuiltQueries[As[F]] {
      type DDL = DDLStatement

      def throwError[A] : A = sys.error(if (errors.isEmpty) "Verification succeeded" else "Verification failed")

      def queries = throwError
      def ddl = throwError
    }
  }

  object zipWithRelation extends Poly2 {
    implicit def findRelation[K, CRD <: HList, RD, Q, T, CR <: HList, KL <: HList, CVL <: HList]
    (implicit ev: Q <:< RelationReference[K],
     s: Selector.Aux[CRD, K, RD],
     ev2: RD <:< RelationDef[T, CR, KL, CVL]) =
      at[CRD, Q] {
      (crd, q) => (q, ev2(s(crd)))
    }
  }

  def buildModelTest[R <: HList, Q <: HList, CRD <: HList, RDQ <: HList,
  QL <: HList, QOut <: HList, As[_[_]], AsRepr <: HList, QOutTag <: HList, RelWithQ <: HList,
  MappedTables <: HList]
  (rm: RelationModel[R, Q, As])
  (implicit
   mapRelations: MapAllRelations.Aux[MapAllContext[HNil, R, ColumnAtom], CRD],
   mapWith: MapWith.Aux[CRD, Q, zipWithRelation.type, RelWithQ],
   tableMap: Mapper.Aux[KeyMapperPoly, RelWithQ, MappedTables],
   buildQueries: Case3.Aux[QueriesPoly, RelWithQ, MappedTables, MapperConfig, BuiltQueries.Aux[QOut, DDLStatement]],
   genAs: Generic.Aux[As[F], AsRepr],
   zip: ZipWithTag.Aux[QOut, AsRepr, QOutTag],
   convert: Mapper.Aux[queriesAs.type, QOutTag, AsRepr]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {
    val withRel = mapWith(mapRelations(MapAllContext(ColumnMapperContext(stdColumnMaker, HNil), rm.relations)), rm.queryList)
    val res = tableMap(withRel)
    val rawQueries = buildQueries(HList(withRel, res, config))
    BuiltQueries(genAs.from(convert(zip(rawQueries.queries))), Eval.later(rawQueries.ddl))
  }

}


