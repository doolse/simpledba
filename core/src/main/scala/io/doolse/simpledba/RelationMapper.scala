package io.doolse.simpledba

import cats.{Applicative, Eval, Monad}
import shapeless._
import shapeless.ops.hlist.Mapper
import shapeless.tag.@@

/**
  * Created by jolz on 10/05/16.
  */

abstract class RelationMapper[F[_]] {

  def M: Applicative[F]
  type DDLStatement
  type KeyMapperT
  type ColumnAtom[A]

  val stdColumnMaker = new MappingCreator[ColumnAtom] {
    def wrapAtom[S, A](atom: ColumnAtom[A], to: (S) => A, from: (A) => S): ColumnAtom[S] = doWrapAtom(atom, to, from)
  }

  def doWrapAtom[S, A](atom: ColumnAtom[A], to: (S) => A, from: (A) => S): ColumnAtom[S]

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
    val rawQueries = convertAndBuild(BuilderContext(M, rm.queryList), relations)
    BuiltQueries(genAs.from(convert(zip(rawQueries.queries))), Eval.later(rawQueries.ddl))
  }



  def verifyModel[R <: HList, Q <: HList, C2, As[_[_]]]
  (rm: RelationModel[R, Q, As], p: String => Unit = Console.err.println)
  (implicit
   verify: ModelVerifier[ModelVerifierContext[R, HNil, ColumnAtom, F, DDLStatement, KeyMapperT, Q, As]]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {

    val errors = verify.errors(
      new ModelVerifierContext(rm, ColumnMapperContext(stdColumnMaker), M)
    )
    errors.foreach(p)
    new BuiltQueries[As[F]] {
      type DDL = DDLStatement

      def throwError[A] : A = sys.error(if (errors.isEmpty) "Verification succeeded" else "Verification failed")

      def queries = throwError
      def ddl = throwError
    }
  }
}


