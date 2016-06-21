package io.doolse.simpledba

import cats.{Applicative, Eval, Unapply}
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist.{Mapper, RightFolder, ZipConst, _}
import shapeless.ops.nat.ToInt
import shapeless.ops.record.{Modifier, Selector, _}
import shapeless.tag._
import BuilderContext._
import fs2.util.Catchable

/**
  * Created by jolz on 8/06/16.
  */

trait BuiltQueries[Q] {
  type DDL

  def queries: Q

  def ddl: List[DDL]
}

object BuiltQueries {
  type Aux[Q, DDL0] = BuiltQueries[Q] {type DDL = DDL0}

  def apply[Q, DDL0](q: Q, _ddl: Eval[List[DDL0]]) : Aux[Q, DDL0] = new BuiltQueries[Q] {
    type DDL = DDL0

    def queries = q

    def ddl = _ddl.value
  }
}

trait BuilderContext[F[_], DDL, KMT, Q] {
  val M: Applicative[F]
  val C: Catchable[F]
  val queries: Q
}

object BuilderContext {
  def apply[F[_], DDL, KMT, Q](_M: Applicative[F], _C: Catchable[F], q: Q) = new BuilderContext[F, DDL, KMT, Q] {
    val M = _M
    val C = _C
    val queries = q
  }

  type PhysRelationOnly[F[_], DDLStatement, T] = PhysRelation[F, DDLStatement, T]
  type PhysRelationAux[F[_], DDLStatement, T, PartKey0, SortKey0] = PhysRelation[F, DDLStatement, T] {
    type PartitionKey = PartKey0
    type SortKey = SortKey0
  }

  type KeyMapperAux[KMT, F[_], DDLStatement, T, CR <: HList, KL <: HList, CVL <: HList, Query, PKN0, PartitionKey0, SKN0, SortKey0]
  = KMT with KeyMapper[T, CR, KL, CVL, Query] {
    type Out = PhysRelationAux[F, DDLStatement, T, PartitionKey0, SortKey0]
    type PartitionKey = PartitionKey0
    type SortKey = SortKey0
    type PartitionKeyNames = PKN0
    type SortKeyNames = SKN0
  }
}

case class PhysicalBuilder[F[_], DDL, T, PKK, PKV, SKK, SKV]
(baseName: String, createPhysical: String => PhysRelationAux[F, DDL, T, PKV, SKV])

case class ReadQueryBuilder[F[_], DDL, T, PKK, PKV, SKK, SKV, Key, RelRequired, Out]
(physical: PhysicalBuilder[F, DDL, T, PKK, PKV, SKK, SKV],
 creatQuery: RelRequired => Out)

class WriteQueryBuilder[F[_], DDL, T](ap: Applicative[F]) {
  def create(tables: List[PhysRelation[F, DDL, T]]): WriteQueries[F, T] = {
    implicit val M = ap
    tables.map(_.createWriteQueries).reduce(WriteQueries.combine[F, T])
  }
}


trait QueriesBuilder[Queries, F[_], DDL] extends DepFn1[Queries] {
  type QOut
  type Out = BuiltQueries.Aux[QOut, DDL]
}

object QueriesBuilder {
  type Aux[Queries, F[_], DDL, QOut0] = QueriesBuilder[Queries, F, DDL] {type QOut = QOut0}

  case class MergedRelations[Full2Builder <: HList, Part2Full <: HList](full2Builder: Full2Builder)

  case class CreatedRelations[RelationMap <: HList, TypeRelations <: HList](relMap: RelationMap, typeRel: TypeRelations)

  trait mergePhysicalLP extends Poly2 {
    implicit def noTables[A, Full2Builder <: HList, Part2Full <: HList] = at[A, MergedRelations[Full2Builder, Part2Full]]((_, o) => o)
  }

  object mergePhysical extends mergePhysicalLP {
    implicit def readQ[F[_], DDL, T, PKK, SKK, PKV, SKV, Key, Req, Out, Full2Builder <: HList, Part2Full <: HList]
    (implicit
     ev: LacksKey[Part2Full, Key],
     update1: Updater[Full2Builder, FieldType[(T, PKK, SKK), PhysicalBuilder[F, DDL, T, PKK, PKV, SKK, SKV]]],
     update2: Updater[Part2Full, FieldType[Key, (T, PKK, SKK)]]
    ) = at[ReadQueryBuilder[F, DDL, T, PKK, PKV, SKK, SKV, Key, Req, Out], MergedRelations[Full2Builder, Part2Full]] {
      (rq, bl) => MergedRelations[update1.Out, update2.Out](update1(bl.full2Builder, field[(T, PKK, SKK)](rq.physical)))
    }
  }

  object createRelations extends Poly2 {

    implicit def writeQIgnore[F[_], DDL, T, W, Out] = at[(WriteQueryBuilder[F, DDL, T], W), Out]((a, b) => b)

    implicit def alreadyExisting[F[_], DDL, T, PKK, SKK, PKV, SKV, Key, InRel, Out, FullKey,
    InKeyMap <: HList, InTypeMap <: HList, F2B <: HList, P2F <: HList]
    (implicit
     fromKeyMap: Selector.Aux[P2F, Key, FullKey],
     relAlready: Selector[InKeyMap, FullKey])
    = at[(ReadQueryBuilder[F, DDL, T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]((a, b) => b)

    implicit def firstEntry[F[_], DDL, T, PKK, PKV, SKK, SKV, Key, InRel, Out, FullKey,
    InKeyMap <: HList, InTypeMap <: HList, F2B <: HList, P2F <: HList]
    (implicit
     fromKeyMap: Selector.Aux[P2F, Key, FullKey],
     ev: LacksKey[InKeyMap, FullKey], ev2: LacksKey[InTypeMap, T],
     selectBuilder: Selector.Aux[F2B, FullKey, PhysicalBuilder[F, DDL, T, PKK, PKV, SKK, SKV]],
     addKey: Updater[InKeyMap, FieldType[(T, PKK, SKK), PhysRelationAux[F, DDL, T, PKV, SKV]]],
     addNewList: Updater[InTypeMap, FieldType[T, PhysRelationOnly[F, DDL, T] :: HNil]]
    )
    = at[(ReadQueryBuilder[F, DDL, T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]({
      case (rqb, cr) =>
        val b = selectBuilder(rqb._2.full2Builder)
        val pt = b.createPhysical(b.baseName)
        CreatedRelations(addKey(cr.relMap, field[(T, PKK, SKK)](pt)), addNewList(cr.typeRel, field[T](pt :: HNil)))
    })

    implicit def anotherEntry[F[_], DDL, T, PKK, PKV, SKK, SKV, Key, InRel, Out, FullKey,
    InKeyMap <: HList, InTypeMap <: HList, F2B <: HList, P2F <: HList, Already <: HList, Len <: Nat]
    (implicit
     fromKeyMap: Selector.Aux[P2F, Key, FullKey],
     selectBuilder: Selector.Aux[F2B, FullKey, PhysicalBuilder[F, DDL, T, PKK, PKV, SKK, SKV]],
     selectExisting: Selector.Aux[InTypeMap, T, Already],
     addKey: Updater[InKeyMap, FieldType[(T, PKK, SKK), PhysRelationAux[F, DDL, T, PKV, SKV]]],
     update: Modifier[InTypeMap, T, Already, PhysRelationOnly[F, DDL, T] :: Already],
     len: Length.Aux[Already, Len],
     toInt: ToInt[Len],
     ev: LacksKey[InKeyMap, FullKey]
    )
    = at[(ReadQueryBuilder[F, DDL, T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]({
      case (rqb, cr) =>
        val already = selectExisting(cr.typeRel)
        val b = selectBuilder(rqb._2.full2Builder)
        val pt = b.createPhysical(s"${b.baseName}_${toInt() + 1}")
        CreatedRelations(addKey(cr.relMap, field[(T, PKK, SKK)](pt)), update(cr.typeRel, pt :: _))
    })
  }

  object buildQueries extends Poly1 {
    implicit def buildReader[F[_], DDL, T, PKK, PKV, SKK, SKV, Key, PhysTable, RelRequired, Out, FKey,
    InKeyMap <: HList, InTypeMap <: HList, P2F <: HList]
    (implicit
     selectFullKey: Selector.Aux[P2F, Key, FKey],
     selectTable: Selector.Aux[InKeyMap, FKey, PhysTable],
     ev: PhysTable <:< RelRequired
    )
    = at[(ReadQueryBuilder[F, DDL, T, PKK, PKV, SKK, SKV, Key, RelRequired, Out], CreatedRelations[InKeyMap, InTypeMap] @@ P2F)] {
      case (rqb, cr) => rqb.creatQuery(selectTable(cr.relMap))
    }

    implicit def buildWriter[F[_], DDL, T, AllTables <: HList, InKeyMap <: HList, InTypeMap <: HList, P2F <: HList]
    (implicit
     selectTables: Selector.Aux[InTypeMap, T, AllTables],
     toList: ToList[AllTables, PhysRelation[F, DDL, T]]
    )
    = at[(WriteQueryBuilder[F, DDL, T], CreatedRelations[InKeyMap, InTypeMap] @@ P2F)] {
      case (wqb, cr) => wqb.create(toList(selectTables(cr.typeRel)))
    }
  }

  implicit def buildAll[F[_], DDL, QB <: HList, MergeOut, WithMerged <: HList,
  WithCreated <: HList, P2F <: HList, F2B <: HList,
  RelationMap <: HList, TypeRelations <: HList, AllRelations <: HList, CROUT]
  (implicit
   merger: RightFolder.Aux[QB, MergedRelations[HNil, HNil], mergePhysical.type, MergeOut],
   ev: MergedRelations[F2B, P2F] =:= MergeOut,
   zipWithMerged: ZipConst.Aux[MergeOut, QB, WithMerged],
   create: RightFolder.Aux[WithMerged, CreatedRelations[HNil, HNil], createRelations.type, CROUT],
   ev2: CROUT <:< CreatedRelations[RelationMap, TypeRelations],
   zipWithCreated: ZipConst.Aux[CreatedRelations[RelationMap, TypeRelations] @@ P2F, QB, WithCreated],
   toPhysList: ToList[RelationMap, PhysRelation[F, DDL, _]],
   bf: Mapper[buildQueries.type, WithCreated])
  = new QueriesBuilder[QB, F, DDL] {
    type QOut = bf.Out

    def apply(qb: QB) = {
      val merged = merger(qb, MergedRelations[HNil, HNil](HNil: HNil))
      val created = create(zipWithMerged(merged, qb), CreatedRelations(HNil, HNil))
      BuiltQueries(
        bf(zipWithCreated(tag[P2F](ev2(created)), qb)),
        Eval.later(toPhysList(created.relMap).map(_.createDDL))
      )
    }

  }
}


private object convertQueries extends Poly2 {
  implicit def convertPartialKey[KMT, F[_], DDL, Q, R <: HList, K, QM, A, CR <: HList, CVL <: HList, KL <: HList, SR, PKK, PKV, SKK, SKV]
  (implicit
   evQM: QM <:< QueryMultiple[K, _, _],
   selRel: Selector.Aux[R, K, SR],
   ev: SR <:< RelationDef[A, CR, KL, CVL],
   keyMapper: KeyMapperAux[KMT, F, DDL, A, CR, KL, CVL, QM, PKK, PKV, SKK, SKV]
  )
  = at[(R, BuilderContext[F, DDL, KMT, Q]), QM] { case ((rels, bc), q) =>
    val rb = ev(selRel(rels))
    implicit val C = bc.C
    ReadQueryBuilder[F, DDL, A, PKK, PKV, SKK, SKV, (A, PKK), PhysRelationAux[F, DDL, A, PKV, SKV], RangeQuery[F, A, PKV, SKV]](
      PhysicalBuilder[F, DDL, A, PKK, PKV, SKK, SKV](rb.baseName, keyMapper.keysMapped(rb.mapper)),
      table => RangeQuery(None, { (pk, l, h, asc) =>
        val rq = table.createReadQueries
        rq.selectMany(table.selectAll, table.whereRange(pk, l, h), asc)
      }))
  }

  implicit def convertFullKey[F[_], DDL, KMT, Q, R <: HList, K, QU, A, PKK, PKV, SKK, SKV, SR, CR <: HList, KL <: HList, CVL <: HList]
  (implicit
   evSQ: QU <:< QueryPK[K],
   selRel: Selector.Aux[R, K, SR],
   ev: SR <:< RelationDef[A, CR, KL, CVL],
   keyMapper: KeyMapperAux[KMT, F, DDL, A, CR, KL, CVL, QU, PKK, PKV, SKK, SKV])
  = at[(R, BuilderContext[F, DDL, KMT, Q]), QU] {
    case ((rels, _), q) =>
      val rb = ev(selRel(rels))
      ReadQueryBuilder[F, DDL, A, PKK, PKV, SKK, SKV, (A, PKK, SKK), PhysRelationAux[F, DDL, A, PKV, SKV],
        UniqueQuery[F, A, PKV :: SKV :: HNil]](
        PhysicalBuilder[F, DDL, A, PKK, PKV, SKK, SKV](rb.baseName, keyMapper.keysMapped(rb.mapper)),
        table => UniqueQuery {
          (fk: PKV :: SKV :: HNil) =>
            val w = table.whereFullKey(fk)
            val rq = table.createReadQueries
            rq.selectOne(table.selectAll, w)
        })
  }

  implicit def convertWrites[F[_], DDL, KMT, Q, R <: HList, K, A, SR]
  (implicit selRel: Selector.Aux[R, K, SR],
   ev: SR <:< RelationDef[A, _, _, _])
  = at[(R,BuilderContext[F, DDL, KMT, Q]), RelationWriter[K]] { case ((_,bc),_) => new WriteQueryBuilder[F, DDL, A](bc.M) }

}

trait ConvertAndBuild[In] extends DepFn1[In]

object ConvertAndBuild {
  type Aux[In, Out0] = ConvertAndBuild[In] {type Out = Out0 }

  implicit def mapAndBuild[F[_], DDL, KMT, CRD <: HList, Q <: HList, QL <: HList, RDQ <: HList, QOut]
  (implicit
   relDefs: ConstMapper.Aux[(CRD, BuilderContext[F, DDL, KMT, Q]), Q, RDQ],
   mapQueries: ZipWith.Aux[RDQ, Q, convertQueries.type, QL],
   queryBuilder: QueriesBuilder.Aux[QL, F, DDL, QOut])
  = new ConvertAndBuild[(BuilderContext[F, DDL, KMT, Q], CRD)] {
    type Out = BuiltQueries.Aux[QOut, DDL]

    def apply(t: (BuilderContext[F, DDL, KMT, Q], CRD)) = t match {
      case (ctx, mappedRels) => queryBuilder(mapQueries(relDefs((mappedRels, ctx), ctx.queries), ctx.queries))
    }
  }
}

trait queriesAsLP extends Poly1 {
  implicit def sq[F[_], FA, FB, T, A, B]
  (implicit ev: FA <:< UniqueQuery[F, T, A], ev2: FB <:< UniqueQuery[F, T, B],
   conv: ValueConvert[B, A]) = at[FA @@ FB] {
    ev(_).as[B]
  }

  implicit def rq[F[_], FA, FB, T, SA, SB, A, B]
  (implicit ev: FA <:< RangeQuery[F, T, A, SA], ev2: FB <:< RangeQuery[F, T, B, SB],
   cv: ValueConvert[B, A], cv2: ValueConvert[SB, SA]) = at[FA @@ FB] {
    ev(_).as[B, SB]
  }

  implicit def mq[F[_], FA, FB, T, A, B, SA]
  (implicit ev: FA <:< RangeQuery[F, T, A, SA], ev2: FB <:< SortableQuery[F, T, B],
   conv: ValueConvert[B, A]) = at[FA @@ FB] { fa =>
    val rq = ev(fa)
    implicit val C = rq.cb
    SortableQuery[F, T, B](None, (b, ascO) => rq._q(conv(b), NoRange, NoRange, ascO))
  }
}

object queriesAs extends queriesAsLP {
  implicit def same[A, B](implicit ev: A <:< B) = at[A @@ B](a => a: B)
}
