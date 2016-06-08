package io.doolse.simpledba

import cats.{Eval, Monad}
import shapeless._
import shapeless.ops.hlist.Mapper
import shapeless.tag.@@

/**
  * Created by jolz on 10/05/16.
  */

abstract class RelationMapper[F[_] : Monad] {

  type DDLStatement
  type KeyMapperT
  type ColumnAtom[A]

  val stdColumnMaker = new MappingCreator[ColumnAtom] {
    def wrapAtom[S, A](atom: ColumnAtom[A], to: (S) => A, from: (A) => S): ColumnAtom[S] = doWrapAtom(atom, to, from)
  }

  def doWrapAtom[S, A](atom: ColumnAtom[A], to: (S) => A, from: (A) => S): ColumnAtom[S]

  trait queriesAsLP extends Poly1 {
    implicit def sq[FA, FB, T, A, B]
    (implicit ev: FA <:< SingleQuery[F, T, A], ev2: FB <:< SingleQuery[F, T, B],
     conv: ValueConvert[B, A]) = at[FA @@ FB] {
      ev(_).as[B]
    }

    implicit def mq[FA, FB, T, A, B]
    (implicit ev: FA <:< MultiQuery[F, T, A], ev2: FB <:< MultiQuery[F, T, B],
     conv: ValueConvert[B, A]) = at[FA @@ FB] {
      ev(_).as[B]
    }
  }

  object queriesAs extends queriesAsLP {
    implicit def same[A, B](implicit ev: A <:< B) = at[A @@ B](a => a: B)
  }

  def buildModel[R <: HList, Q <: HList, CRD <: HList, RDQ <: HList,
  QL <: HList, QOut <: HList, As[_[_]], AsRepr <: HList, QOutTag <: HList]
  (rm: RelationModel[R, Q, As])
  (implicit
   mapRelations: MapAllRelations.Aux[MapAllContext[HNil, R, ColumnAtom], CRD],
   convertAndBuild: ConvertAndBuild.Aux[(CRD, Q), F, DDLStatement, KeyMapperT, QOut],
   genAs: Generic.Aux[As[F], AsRepr],
   zip: ZipWithTag.Aux[QOut, AsRepr, QOutTag],
   convert: Mapper.Aux[queriesAs.type, QOutTag, AsRepr]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {
    val relations = mapRelations(MapAllContext(ColumnMapperContext(stdColumnMaker, HNil), rm.relations))
    val rawQueries = convertAndBuild(relations, rm.queryList)
    BuiltQueries(genAs.from(convert(zip(rawQueries.queries))), rawQueries.ddl)
  }



  def verifyModel[R <: HList, Q <: HList, C2, As[_[_]]]
  (rm: RelationModel[R, Q, As], p: String => Unit = Console.err.println)
  (implicit
   verify: ModelVerifier[ModelVerifierContext[F, DDLStatement, KeyMapperT, R, Q, As, ColumnAtom, MapAllContext[HNil, R, ColumnAtom]]]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {

    verify.errors(
      ModelVerifierContext(rm, MapAllContext(ColumnMapperContext(stdColumnMaker), rm.relations))
    ).foreach(p)

    new BuiltQueries[As[F]] {
      type DDL = DDLStatement
      def queries: As[F] = sys.error("Please read verification")

      def ddl: Eval[List[DDLStatement]] = sys.error("Please read verification")
    }
  }
}


