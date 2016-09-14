package io.doolse.simpledba

import shapeless.ops.hlist._
import shapeless.{::, HList, HNil, Poly1, Witness}
import shapeless.ops.record.{Keys, LacksKey, Selector}
import shapeless.poly.Case1

/**
  * Created by jolz on 8/06/16.
  */

case class QueryBuilderVerifierContext[RM <: HList, KeyMapperPoly](relations: RM)

case class QueryBuilderVerifier[In](errors: In => List[String])

trait QueryBuilderVerifierLP {

  implicit def missingRelation[RM <: HList, Q, K <: Symbol, KMT]
  (implicit
   ev: Q <:< RelationQuery[K],
   lk: LacksKey[RM, K],
   key: Witness.Aux[K]
  ) = QueryBuilderVerifier[(QueryBuilderVerifierContext[RM, KMT], Q)](_ => List(s"No relation for ${key.value.name}"))

  object errorMessage extends Poly1 {

    def columns2String(cols: List[Symbol]) = cols.mkString("(", ",", ")")

    def relationString(name: Symbol, allCols: List[Symbol], pk: List[Symbol]) =
      s"${name.name} PK ${columns2String(pk)} - ${columns2String(allCols.filterNot(pk.toSet))}"

    implicit def unique[K <: Symbol]
    (implicit
     tn: Witness.Aux[K]) = at[(QueryPK[K], List[Symbol], List[Symbol])] {
      case (qu, allCols, pk) =>
        s"Failed to create primary key result mapping on relation: ${relationString(tn.value, allCols, pk)}"
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

  implicit def failedKeyMapper[RM <: HList, Q, K <: Symbol, RD,
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
  ) = QueryBuilderVerifier[(QueryBuilderVerifierContext[RM, KMT], Q)] { in =>
    val allCols = allColList(allColKeys())
    val pkCols = allPKNames(pkLRec())
    List(toError(in._2, allCols, pkCols))
  }
}

object QueryBuilderVerifier extends QueryBuilderVerifierLP {

  implicit def verifyWrites[RM <: HList, Q, K, KMT]
  (implicit
   ev: Q <:< RelationWriter[K],
   lk: Selector[RM, K]) = QueryBuilderVerifier[(QueryBuilderVerifierContext[RM, KMT], Q)](_ => List.empty)

  implicit def verifyQuery[RM <: HList, K, RD, T, CR <: HList, KL <: HList, CVL <: HList, Q, KMT]
  (implicit
   ev2: Q <:< RelationQuery[K],
   sel: Selector.Aux[RM, K, RD],
   ev: RD <:< RelationDef[T, CR, KL, CVL],
   c: Case1[KMT, (Q, RD)]
  ) = QueryBuilderVerifier[(QueryBuilderVerifierContext[RM, KMT], Q)](_ => List.empty)

  implicit def hconsVerify[CTX, H, T <: HList](implicit hv: QueryBuilderVerifier[(CTX, H)], tv: QueryBuilderVerifier[(CTX, T)])
  = QueryBuilderVerifier[(CTX, H :: T)] {
    case (rm, h :: t) => hv.errors(rm, h) ++ tv.errors(rm, t)
  }

  implicit def hnilVerify[RM] = QueryBuilderVerifier[(RM, HNil)](_ => List.empty)
}

