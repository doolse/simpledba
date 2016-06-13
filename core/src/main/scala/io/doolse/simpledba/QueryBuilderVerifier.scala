package io.doolse.simpledba

import shapeless.ops.hlist._
import shapeless.{::, HList, HNil, Poly1, Witness}
import shapeless.ops.record.{Keys, LacksKey, Selector}

/**
  * Created by jolz on 8/06/16.
  */

case class QueryBuilderVerifierContext[F[_], DDL, RM <: HList, KeyMapperT](relations: RM)

case class QueryBuilderVerifier[In](errors: In => List[String])

trait QueryBuilderVerifierLP {

  implicit def missingRelation[F[_], DDL, RM <: HList, Q, K <: Symbol, KMT]
  (implicit
   ev: Q <:< RelationQuery[K],
   lk: LacksKey[RM, K],
   key: Witness.Aux[K]
  ) = QueryBuilderVerifier[(QueryBuilderVerifierContext[F, DDL, RM, KMT], Q)](_ => List(s"No relation for ${key.value.name}"))

  object errorMessage extends Poly1 {

    def columns2String(cols: List[Symbol]) = cols.mkString("(", ",", ")")

    def relationString(name: Symbol, allCols: List[Symbol], pk: List[Symbol]) =
      s"${name.name} PK ${columns2String(pk)} - ${columns2String(allCols.filterNot(pk.toSet))}"

    implicit def unique[K <: Symbol, Cols <: HList, ColsR <: HList]
    (implicit
     uniqueCols: Reify.Aux[Cols, ColsR],
     toList: ToList[ColsR, Symbol],
     tn: Witness.Aux[K]) = at[(QueryUnique[K, Cols], List[Symbol], List[Symbol])] {
      case (qu, allCols, pk) =>
        s"Failed to find unique result mapping on relation: ${relationString(tn.value, allCols, pk)}" +
          s" - using columns ${columns2String(toList(uniqueCols()))}"
    }

    implicit def multiple[K <: Symbol, Cols <: HList, ColsR <: HList, Sort <: HList, SortR <: HList]
    (implicit
     uniqueCols: Reify.Aux[Cols, ColsR],
     sortCols: Reify.Aux[Sort, SortR],
     toList: ToList[ColsR, Symbol],
     sortToList: ToList[SortR, Symbol],
     tn: Witness.Aux[K]) = at[(QueryMultiple[K, Cols, Sort], List[Symbol], List[Symbol])] {
      case (qu, allCols, pk) => s"Failed to find multi result mapping on relation: ${relationString(tn.value, allCols, pk)}" +
        s" - using columns ${columns2String(toList(uniqueCols()))} sort ${columns2String(sortToList(sortCols()))}"
    }
  }

  implicit def failedKeyMapper[F[_], DDL, RM <: HList, Q, K <: Symbol, RD,
  T, CR <: HList, KL <: HList, KLL <: HList, CVL <: HList, CRK <: HList, KMT]
  (implicit
   ev: Q <:< RelationQuery[K],
   selectRelation: Selector.Aux[RM, K, RD],
   evRD: RD <:< RelationDef[T, CR, KL, _],
   allColKeys: Keys.Aux[CR, CRK],
   allColList: ToList[CRK, Symbol],
   pkLRec: Reify.Aux[KL, KLL],
   allPKNames: ToList[KLL, Symbol],
   toError: errorMessage.Case.Aux[(Q, List[Symbol], List[Symbol]), String]
  ) = QueryBuilderVerifier[(QueryBuilderVerifierContext[F, DDL, RM, KMT], Q)] { in =>
    val allCols = allColList(allColKeys())
    val pkCols = allPKNames(pkLRec())
    List(toError(in._2, allCols, pkCols))
  }
}

object QueryBuilderVerifier extends QueryBuilderVerifierLP {

  type KeyMapperLookup[KMT, T, CR <: HList, KL <: HList, CVL <: HList, Query]
  = KMT with KeyMapper[T, CR, KL, CVL, Query]

  implicit def verifyWrites[F[_], DDL, RM <: HList, Q, K, KMT]
  (implicit
   ev: Q <:< RelationWriter[K],
   lk: Selector[RM, K]) = QueryBuilderVerifier[(QueryBuilderVerifierContext[F, DDL, RM, KMT], Q)](_ => List.empty)

  implicit def verifyQuery[F[_], DDL, RM <: HList, K, RD, T, CR <: HList, KL <: HList, CVL <: HList, Q, KMT]
  (implicit
   ev2: Q <:< RelationQuery[K],
   sel: Selector.Aux[RM, K, RD],
   ev: RD <:< RelationDef[T, CR, KL, CVL],
   km: KeyMapperLookup[KMT, T, CR, KL, CVL, Q],
   c: convertQueries.Case[(RM, BuilderContext[F, DDL, KMT, HNil]), Q]
  ) = QueryBuilderVerifier[(QueryBuilderVerifierContext[F, DDL, RM, KMT], Q)](_ => List.empty)

  implicit def hconsVerify[CTX, H, T <: HList](implicit hv: QueryBuilderVerifier[(CTX, H)], tv: QueryBuilderVerifier[(CTX, T)])
  = QueryBuilderVerifier[(CTX, H :: T)] {
    case (rm, h :: t) => hv.errors(rm, h) ++ tv.errors(rm, t)
  }

  implicit def hnilVerify[RM] = QueryBuilderVerifier[(RM, HNil)](_ => List.empty)
}
