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


object RelationMapper {

  case class TableNameDetails(existingTables: Set[String], baseName: String, nameHint: Option[String], pkNames: Seq[String], skNames: Seq[String])

  case class SimpleMapperConfig(tableNamer: TableNameDetails => String)


  def prefixTableNamer(prefix: String) =
    (td: TableNameDetails) => {
      val bn = prefix + td.baseName
      val existing = td.existingTables
      val withHint = td.nameHint.map(h => bn + h).getOrElse(bn)
      if (!existing(withHint)) withHint
      else {
          (2 to 1000).iterator.map(n => s"${withHint}_$n")
            .find(n => !existing(n))
            .getOrElse(sys.error(s"Couldn't generate a unique table name for $withHint"))
      }
    }

  val defaultMapperConfig = SimpleMapperConfig(prefixTableNamer(""))
}

abstract class RelationMapper[F[_]] {

  type DDLStatement
  type ColumnAtom[A]
  type MapperConfig
  type KeyMapperPoly <: Poly1
  type QueriesPoly <: Poly3

  val config: MapperConfig

  def stdColumnMaker: MappingCreator[ColumnAtom]

  def verifyModel[R <: HList, Q <: HList, C2, As[_[_]]]
  (rm: RelationModel[R, Q, As], p: String => Unit = Console.err.println)
  (implicit
   verify: ModelVerifier[ModelVerifierContext[R, HNil, ColumnAtom, F, DDLStatement, KeyMapperPoly, QueriesPoly, Q, As, MapperConfig]]
  ): BuiltQueries.Aux[As[F], DDLStatement] = {

    val (name, errors) = verify.errors(
      new ModelVerifierContext(rm, ColumnMapperContext(stdColumnMaker), config)
    )
    p(name)
    errors.foreach(p)
    new BuiltQueries[As[F]] {
      type DDL = DDLStatement

      def throwError[A]: A = sys.error(if (errors.isEmpty) "Verification succeeded" else "Verification failed")

      def queries = throwError

      def ddl = throwError
    }
  }

  def buildModel[R <: HList, Q <: HList, CRD <: HList, RDQ <: HList,
  QL <: HList, QOut <: HList, As[_[_]], AsRepr <: HList, QOutTag <: HList,
  RelWithQ <: HList, MappedTables <: HList]
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


