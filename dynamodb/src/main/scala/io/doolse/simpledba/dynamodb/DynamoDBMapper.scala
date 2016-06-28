package io.doolse.simpledba.dynamodb

import cats.{Applicative, Eval, Monad}
import cats.data.{Reader, ReaderT, Xor}
import cats.syntax.all._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import fs2.interop.cats._
import fs2._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{Stream => _, _}
import fs2.util.{Catchable, Task}
import io.doolse.simpledba.PhysRelation.Aux
import io.doolse.simpledba.RelationMapper._
import io.doolse.simpledba._
import io.doolse.simpledba.dynamodb.DynamoDBMapper._
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist
import shapeless.ops.hlist.{Collect, Diff, Intersection, IsHCons, Length, Prepend, RemoveAll, RightFolder, Take, ToList}
import shapeless.ops.record.{Keys, SelectAll, Selector}
import poly.Case1

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 12/05/16.
  */

case class DynamoDBSession(client: AmazonDynamoDBAsync)

object DynamoDBMapper {
  type Effect[A] = ReaderT[Task, DynamoDBSession, A]
  type DynamoDBDDL = CreateTableRequest


  def asAttrMap(l: Seq[PhysicalValue[DynamoDBColumn]]) = l.map(physical2Attribute).toMap.asJava

  def physical2Attribute(pc: PhysicalValue[DynamoDBColumn]) = pc.name -> pc.atom.to(pc.v)

  implicit val strat = Strategy.fromExecutionContext(ExecutionContext.global)

  def asyncWR[R <: AmazonWebServiceRequest, A](r: R, f: (R, AsyncHandler[R, A]) => java.util.concurrent.Future[A]): Task[A] = {
    Task.async[A] {
      cb => f(r, new AsyncHandler[R, A] {
        def onError(exception: Exception): Unit = cb(Left(exception))

        def onSuccess(request: R, result: A): Unit = cb(Right(result))
      })
    }
  }

  def asValueUpdate(d: ValueDifference[DynamoDBColumn]) = {
    d.name -> d.atom.diff(d.existing, d.newValue)
  }

  def createMaterializer(m: java.util.Map[String, AttributeValue]) = new ColumnMaterialzer[DynamoDBColumn] {
    def apply[A](name: String, atom: DynamoDBColumn[A]): Option[A] = {
      Option(m.get(name)).map(av => atom.from(av))
    }
  }

  def resultStream(qr: QueryRequest): Stream[Effect, java.util.Map[String, AttributeValue]] = {
    val request = Stream.eval[Effect, QueryResult](ReaderT(s => asyncWR(qr, s.client.queryAsync)))
    request.flatMap {
      result =>
        val chunk = Stream.chunk(Chunk.seq(result.getItems.asScala))
        if (result.getLastEvaluatedKey == null) chunk
        else chunk ++ resultStream(qr.withExclusiveStartKey(result.getLastEvaluatedKey))
    }
  }

}

trait QueryParam {
  def columnName: (String, String)

  def values: Iterable[(String, AttributeValue)]

  def expr: String
}

case class SimpleParam(name: String, v: PhysicalValue[DynamoDBColumn], op: String) extends QueryParam {
  val columnName = s"#$name" -> v.name
  val value = s":$name" -> v.atom.to(v.v)
  val values = Iterable(value)

  def expr = s"${columnName._1} $op ${value._1}"
}

case class BetweenParam(name: String, v: PhysicalValue[DynamoDBColumn], v2: PhysicalValue[DynamoDBColumn]) extends QueryParam {
  val columnName = s"#$name" -> v.name
  val value1 = s":${name}1" -> v.atom.to(v.v)
  val value2 = s":${name}2" -> v2.atom.to(v2.v)
  val values = Iterable(value1, value2)

  def expr = s"${columnName._1} BETWEEN ${value1._1} AND ${value2._1}"
}

sealed trait DynamoProjection[T] {
  def materialize: ColumnMaterialzer[DynamoDBColumn] => Option[T]
}

case class All[T, CVL <: HList](materialize: ColumnMaterialzer[DynamoDBColumn] => Option[T]) extends DynamoProjection[T]

sealed trait DynamoWhere {
  def forRequest: QueryRequest => QueryRequest

  def asMap: java.util.Map[String, AttributeValue]
}

case class KeyMatch(pk: PhysicalValue[DynamoDBColumn], sk1: Option[(PhysicalValue[DynamoDBColumn], String)],
                    sk2: Option[(PhysicalValue[DynamoDBColumn], String)]) extends DynamoWhere {
  def forRequest = qr => {
    val opSK = (sk1, sk2) match {
      case (Some((pv, op1)), Some((pv2, op2))) => Option(BetweenParam("SK1", pv, pv2))
      case _ => sk1.orElse(sk2) map { s => SimpleParam("SK1", s._1, s._2) }
    }
    val params = List(SimpleParam("PK", pk, "=")) ++ opSK
    val expr = params.map(_.expr).mkString(" AND ")
    val nameMap = params.map(_.columnName).toMap.asJava
    val valMap = params.flatMap(_.values).toMap.asJava
    qr.withKeyConditionExpression(expr)
      .withExpressionAttributeNames(nameMap)
      .withExpressionAttributeValues(valMap)
  }

  def asMap = asAttrMap(List(pk) ++ sk1.map(_._1) ++ sk2.map(_._1))
}

class DynamoDBMapper(val config: SimpleMapperConfig = defaultMapperConfig) extends RelationMapper[DynamoDBMapper.Effect] {

  type ColumnAtom[A] = DynamoDBColumn[A]
  type MapperConfig = SimpleMapperConfig
  type DDLStatement = DynamoDBDDL
  type KeyMapperT = DynamoDBKeyMapper
  type KeyMapperPoly = DynamoDBKeyMapper2.type
  type QueriesPoly = ConvertQueries2.type

  val stdColumnMaker = new MappingCreator[DynamoDBColumn] {
    def wrapAtom[S, A](atom: DynamoDBColumn[A], to: (S) => A, from: (A) => S): DynamoDBColumn[S] = {
      val (lr, hr) = atom.range
      DynamoDBColumn[S](
        atom.from andThen from,
        atom.to compose to,
        atom.attributeType, (ex, nv) => atom.diff(to(ex), to(nv)), atom.sortablePart compose to, (from(lr), from(hr)))
    }
  }

  def M = Applicative[Effect]

  def C = implicitly[Catchable[Effect]]

}

trait DynamoDBPhysicalRelations[T, CR <: HList, CVL <: HList, PKN, SKN, PKVO, SKVO] {
  type PKVRaw
  type SKVRaw

  def apply(colMapper: ColumnMapper[T, CR, CVL], tableName: String, convPK: PKVO => PKVRaw, convSK: (SKVO, Boolean) => SKVRaw)
  : PhysRelation.Aux[DynamoDBMapper.Effect, CreateTableRequest, T, PKVO, SKVO]
}

object DynamoDBPhysicalRelations {

  type Aux[T, CR <: HList, CVL <: HList, PKN, SKN, PKVO, SKVO, PKVRaw0, SKVRaw0] = DynamoDBPhysicalRelations[T, CR, CVL, PKN, SKN, PKVO, SKVO] {
    type PKVRaw = PKVRaw0
    type SKVRaw = SKVRaw0
  }

  implicit def dynamoDBTable[T, CR <: HList, CVL <: HList, PKK, SKKL <: HList,
  PKV0, SKV0, SKCL <: HList, PKC, PKV, SKV]
  (implicit
   selectPkCol: Selector.Aux[CR, PKK, PKC],
   skSel: SelectAll.Aux[CR, SKKL, SKCL],
   pkValB: PhysicalValues.Aux[DynamoDBColumn, PKC, PKV0, PhysicalValue[DynamoDBColumn]],
   skToList: ToList[SKCL, ColumnMapping[DynamoDBColumn, T, _]],
   evCol: PKC =:= ColumnMapping[DynamoDBColumn, T, PKV0],
   skValB: PhysicalValues.Aux[DynamoDBColumn, SKCL, SKV0, List[PhysicalValue[DynamoDBColumn]]],
   helperB: ColumnListHelperBuilder[DynamoDBColumn, T, CR, CVL, PKV0 :: SKV0 :: HNil],
   extractVals: ValueExtractor.Aux[CR, CVL, PKK :: SKKL :: HNil, PKV0 :: SKV0 :: HNil]
  ) = new DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKKL, PKV, SKV] {
    type PKVRaw = PKV0
    type SKVRaw = SKV0

    def apply(colMapper: ColumnMapper[T, CR, CVL], tableName: String,
              convPK: PKV => PKVRaw, convSK: (SKV, Boolean) => SKVRaw): PhysRelation.Aux[DynamoDBMapper.Effect, CreateTableRequest, T, PKV, SKV]
    = new PhysRelation[Effect, CreateTableRequest, T] {
      self =>
      type PartitionKey = PKV
      type SortKey = SKV
      type Projection[A] = DynamoProjection[A]
      type Where = DynamoWhere
      val toKeys = extractVals()
      val helper = helperB(colMapper, toKeys)
      val pkCol = selectPkCol(colMapper.columns)
      val skColL = skSel(colMapper.columns)
      val pkVals = pkValB(pkCol)
      val skVals = skValB(skColL)

      def keysAsAttributes(keys: PKVRaw :: SKVRaw :: HNil) =
        keyMatchFromList(keys).asMap

      def keyMatchFromList(keys: PKVRaw :: SKVRaw :: HNil) =
        KeyMatch(pkVals(keys.head), skVals(keys.tail.head).headOption.map((_, "=")), None)

      def createKeyMatch(pk: PartitionKey, sk: SortKey): DynamoWhere
      = keyMatchFromList(convPK(pk) :: convSK(sk, true) :: HNil)

      def whereFullKey(fk: PartitionKey :: SortKey :: HNil): DynamoWhere = createKeyMatch(fk.head, fk.tail.head)

      def wherePK(pk: PartitionKey): DynamoWhere = KeyMatch(pkVals(convPK(pk)), None, None)

      def rangeToSK(opInc: String, opExc: String, lower: Boolean, rv: RangeValue[SortKey]): Option[(PhysicalValue[DynamoDBColumn], String)] = (rv match {
        case NoRange => None
        case Inclusive(a) => Some((a, opInc, lower))
        case Exclusive(a) => Some((a, opExc, !lower))
      }).flatMap {
        case (a, op, l) => skVals(convSK(a, l)).headOption.map((_, op))
      }

      def whereRange(pk: PartitionKey, lower: RangeValue[SortKey], upper: RangeValue[SortKey]): DynamoWhere =
        KeyMatch(pkVals(convPK(pk)), rangeToSK(">=", ">", true, lower), rangeToSK("<=", "<", false, upper))

      def createReadQueries: ReadQueries = new ReadQueries {
        def selectOne[A](projection: DynamoProjection[A], where: DynamoWhere): Effect[Option[A]] = ReaderT { s =>
          asyncWR(new GetItemRequest(tableName, where.asMap), s.client.getItemAsync).map {
            (gir: GetItemResult) => Option(gir.getItem()).map(createMaterializer).flatMap(projection.materialize)
          }
        }


        //        def doQuery(qr: QueryRequest): Effect[QueryResult] = ReaderT { s => asyncWR(qr, s.client.queryAsync) }

        def selectMany[A](projection: DynamoProjection[A], where: DynamoWhere, asc: Option[Boolean]): Stream[Effect, A] = {
          val qr = where.forRequest(new QueryRequest().withTableName(tableName))
          val qr2 = asc.map(a => qr.withScanIndexForward(a)).getOrElse(qr)

          resultStream(qr2).map(map => projection.materialize(createMaterializer(map))).collect {
            case Some(a) => a
          }
        }
      }

      def createWriteQueries: WriteQueries[Effect, T] = new WriteQueries[Effect, T] {
        def delete(t: T): Effect[Unit] = ReaderT { s =>
          asyncWR(new DeleteItemRequest(tableName, keysAsAttributes(toKeys(colMapper.toColumns(t)))), s.client.deleteItemAsync).map(_ => ())
        }

        def insert(t: T): Effect[Unit] = ReaderT { s =>
          asyncWR(new PutItemRequest(tableName, asAttrMap(helper.toPhysicalValues(t))), s.client.putItemAsync).map(_ => ())
        }

        def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
          helper.changeChecker(existing, newValue).map {
            case Xor.Left((k, changes)) =>
              asyncWR(new UpdateItemRequest(tableName, keysAsAttributes(k), changes.map(c => asValueUpdate(c)).toMap.asJava), s.client.updateItemAsync)
            case Xor.Right((oldKey, newk, vals)) =>
              asyncWR(new DeleteItemRequest(tableName, keysAsAttributes(oldKey)), s.client.deleteItemAsync) <*
                asyncWR(new PutItemRequest(tableName, asAttrMap(vals)), s.client.putItemAsync)
          } map (_.map(_ => true)) getOrElse Task.now(false)
        }
      }

      def createDDL: CreateTableRequest = {
        val keyColumn = evCol(pkCol)
        val sortKeyList = skToList(skColL)
        val sortDef = (List(keyColumn) ++ sortKeyList).map(c => new AttributeDefinition(c.name, c.atom.attributeType))
        val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
        val sortKeyDef = sortKeyList.headOption.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
        new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))
      }

      def selectAll: DynamoProjection[T] = All(helper.materializer)
    }
  }
}

trait DynamoDBKeyMapper

trait DynamoDBKeyMapperLP {
  val PKComposite: Witness = 'PK_composite
  val SKComposite: Witness = 'SK_composite

  val asString: PhysicalValue[DynamoDBColumn] => String = db => db.atom.sortablePart(db.v)

  def combine(l: List[PhysicalValue[DynamoDBColumn]]) = l.map(asString).mkString(",")

  implicit def compositePrimaryKey[K, T, CR <: HList, CVL <: HList, PKL <: HList, PKV, PKLCL <: HList, UPCR <: HList]
  (implicit
   selPKL: SelectAll.Aux[CR, PKL, PKLCL],
   pkVals: PhysicalValues.Aux[DynamoDBColumn, PKLCL, PKV, List[PhysicalValue[DynamoDBColumn]]],
   extractPKL: ValueExtractor.Aux[CR, CVL, PKL, PKV],
   prependComposite: Prepend.Aux[FieldType[PKComposite.T, ColumnMapping[DynamoDBColumn, T, String]] :: HNil, CR, UPCR],
   relMaker: DynamoDBPhysicalRelations.Aux[T, UPCR, String :: CVL, PKComposite.T, HNil, PKV, HNil, String, HNil]
  ): DynamoDBKeyMapper.Aux[T, CR, PKL, CVL, QueryPK[K], PKL, HNil, PKV, HNil]
  = new DynamoDBKeyMapper with KeyMapper.Impl[T, CR, PKL, CVL, QueryPK[K], PKL,
    PKV, HNil, HNil, PhysRelation.Aux[Effect, CreateTableRequest, T, PKV, HNil]] {
    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String) = {

      val columns = cm.columns
      val getPKV = extractPKL()
      val pkvToString = pkVals(selPKL(columns)) andThen combine

      val pkField = field[PKComposite.T](ColumnMapping[DynamoDBColumn, T, String]("PK_composite", DynamoDBColumn.stringColumn,
        cm.toColumns andThen getPKV andThen pkvToString))

      val cm2 = ColumnMapper[T, UPCR, String :: CVL](prependComposite(pkField :: HNil, columns),
        cvlWithComp => cm.fromColumns(cvlWithComp.tail), { t =>
          val cvl = cm.toColumns(t)
          pkvToString(getPKV(cvl)) :: cvl
        }
      )
      relMaker(cm2, name, pkvToString, (skv, l) => HNil)
    }
  }


  implicit def compositeKeys[K, T, CR <: HList, CVL <: HList, PKL <: HList,
  UCL <: HList, SKL <: HList, UPCR1 <: HList, UPCR2 <: HList, PKV, SKV,
  UCLL <: HList, SKLL <: HList,
  ICL <: HList, PKWithoutUCL <: HList, ISKL <: HList,
  XSKL <: HList, XSKLCL <: HList, XSKV]
  (implicit
   selUCL: SelectAll.Aux[CR, UCL, UCLL],
   selSKL: SelectAll.Aux[CR, SKL, SKLL],
   uclVals: PhysicalValues.Aux[DynamoDBColumn, UCLL, PKV, List[PhysicalValue[DynamoDBColumn]]],
   sklVals: PhysicalValues.Aux[DynamoDBColumn, SKLL, SKV, List[PhysicalValue[DynamoDBColumn]]],
   extractUCL: ValueExtractor.Aux[CR, CVL, UCL, PKV],
   intersect: Intersection.Aux[PKL, UCL, ICL],
   pkWithOutSC: RemoveAll.Aux[PKL, ICL, (ICL, PKWithoutUCL)],
   intersectSort: Intersection.Aux[PKWithoutUCL, SKL, ISKL],
   pkWithOutSK: RemoveAll.Aux[PKWithoutUCL, ISKL, (ISKL, XSKL)],
   selXSKL: SelectAll.Aux[CR, XSKL, XSKLCL],
   xskList: ToList[XSKLCL, ColumnMapping[DynamoDBColumn, T, _]],
   xskVals: PhysicalValues.Aux[DynamoDBColumn, XSKLCL, XSKV, List[PhysicalValue[DynamoDBColumn]]],
   extractSKL: ValueExtractor.Aux[CR, CVL, SKL :: XSKL :: HNil, SKV :: XSKV :: HNil],
   update1: Prepend.Aux[FieldType[PKComposite.T, ColumnMapping[DynamoDBColumn, T, String]] ::
     FieldType[SKComposite.T, ColumnMapping[DynamoDBColumn, T, String]] :: HNil, CR, UPCR1],
   relMaker: DynamoDBPhysicalRelations.Aux[T, UPCR1, String :: String :: CVL, PKComposite.T, SKComposite.T :: HNil,
     PKV, SKV, String, String :: HNil]
  )
  : DynamoDBKeyMapper.Aux[T, CR, PKL, CVL, QueryMultiple[K, UCL, SKL], UCL, SKL, PKV, SKV]
  = new DynamoDBKeyMapper with KeyMapper.Impl[T, CR, PKL, CVL, QueryMultiple[K, UCL, SKL], UCL,
    PKV, SKL, SKV, PhysRelation.Aux[Effect, CreateTableRequest, T, PKV, SKV]] {

    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String) = {

      val columns = cm.columns
      def xskOK(lower: Boolean) = xskList(selXSKL(columns)).map { cm =>
        PhysicalValue(cm.name, cm.atom, if (lower) cm.atom.range._1 else cm.atom.range._2)
      }
      val getPKV = extractUCL()
      val getAllSKV = extractSKL()
      val pkvToString = uclVals(selUCL(columns)) andThen combine
      def skvToString(v: SKV :: XSKV :: HNil) =
        combine(sklVals(selSKL(columns)).apply(v.head) ++ xskVals(selXSKL(columns)).apply(v.tail.head))
      def skvOnlyToString(v: SKV, lower: Boolean) =
        combine(sklVals(selSKL(columns))(v) ++ xskOK(lower))

      val pkField = field[PKComposite.T](ColumnMapping[DynamoDBColumn, T, String]("PK_composite", DynamoDBColumn.stringColumn,
        cm.toColumns andThen getPKV andThen pkvToString))
      val skField = field[SKComposite.T](ColumnMapping[DynamoDBColumn, T, String]("SK_composite", DynamoDBColumn.stringColumn,
        cm.toColumns andThen getAllSKV andThen skvToString))

      val cm2 = ColumnMapper[T, UPCR1, String :: String :: CVL](update1(pkField :: skField :: HNil, columns),
        cvl => cm.fromColumns(cvl.tail.tail), { t =>
          val cvl = cm.toColumns(t)
          pkvToString(getPKV(cvl)) :: skvToString(getAllSKV(cvl)) :: cvl
        }
      )
      relMaker(cm2, name, pkvToString, (skv, l) => skvOnlyToString(skv, l) :: HNil)
    }
  }

}

object DynamoDBKeyMapper extends DynamoDBKeyMapperLP {

  type Aux[T, CR <: HList, KL <: HList, CVL <: HList, Q, PKK, SKKL, PKV, SKV] = DynamoDBKeyMapper with KeyMapper[T, CR, KL, CVL, Q] {
    type Out = PhysRelation.Aux[Effect, CreateTableRequest, T, PKV, SKV]
    type PartitionKey = PKV
    type SortKey = SKV
    type PartitionKeyNames = PKK
    type SortKeyNames = SKKL
  }

  def apply[T, CR <: HList, KL <: HList, CVL <: HList, Q, PKK, SKKL, PKV, SKV]
  (relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKKL, PKV, SKV, PKV, SKV])
  : Aux[T, CR, KL, CVL, Q, PKK, SKKL, PKV, SKV]
  = new DynamoDBKeyMapper with KeyMapper.Impl[T, CR, KL, CVL, Q, PKK,
    PKV, SKKL, SKV, PhysRelation.Aux[Effect, CreateTableRequest, T, PKV, SKV]] {

    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String) = relMaker(cm, name, identity, (a, b) => a)
  }

  implicit def primaryKey[K, T, CR <: HList, CVL <: HList, PKK, PKV, PKC]
  (implicit
   selPK: Selector.Aux[CR, PKK, PKC],
   cv: ColumnValues.Aux[PKC, PKV],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, HNil, PKV, HNil, PKV, HNil]
  ): Aux[T, CR, PKK :: HNil, CVL, QueryPK[K], PKK, HNil, PKV, HNil]
  = DynamoDBKeyMapper(relMaker)

  implicit def twoKeys[K, T, CR <: HList, CVL <: HList, PKK, PKV, SKK, SKV, PKC, SKC]
  (implicit
   selPK: Selector.Aux[CR, PKK, PKC],
   cv: ColumnValues.Aux[PKC, PKV],
   selSK: Selector.Aux[CR, SKK, SKC],
   cvSK: ColumnValues.Aux[SKC, SKV],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV :: HNil, PKV, SKV :: HNil]
  ): Aux[T, CR, PKK :: SKK :: HNil, CVL, QueryPK[K], PKK, SKK :: HNil, PKV, SKV :: HNil]
  = DynamoDBKeyMapper(relMaker)

  implicit def multiNoSort[K, T, CR <: HList, CVL <: HList, KL <: HList, PKK, PKV, SKL, SKV]
  (implicit
   remPK: hlist.Remove.Aux[KL, PKK, (PKK, SKL)],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKL, PKV, SKV, PKV, SKV]
  ): Aux[T, CR, KL, CVL, QueryMultiple[K, PKK :: HNil, HNil], PKK, SKL, PKV, SKV]
  = DynamoDBKeyMapper(relMaker)
}

case class DynamoTable[K, T, PKL, SKL, PKV, SKV](helper: ColumnFamilyHelper[DynamoDBColumn, T, PKV, SKV],
                                                 name: String = "") extends TableWithSK[SKL] {
  def atomForKey(prefix: String, cols: Seq[ColumnMapping[DynamoDBColumn, T, _]]) = if (cols.length > 1)
    Some(ColumnMapping[DynamoDBColumn, T, String](prefix + "_Composite", DynamoDBColumn.stringColumn, _ => ???))
  else cols.headOption

  lazy val pkAtom = atomForKey("PK", helper.pkColumns).get
  lazy val skAtom = atomForKey("SK", helper.skColumns)

  def compositeValue(prefix: String, vals: Seq[PhysicalValue[DynamoDBColumn]]) = if (vals.length > 1) {
    Some(PhysicalValue(prefix + "_Composite", DynamoDBColumn.stringColumn, vals.map(pv => pv.atom.sortablePart(pv.v)).mkString(",")))
  } else None

  def toPhysicalValues(t: T) = {
    val (all, pk, sk) = helper.toAllPhysicalValues(t)
    all ++ compositeValue("PK", pk) ++ compositeValue("SK", sk)
  }

  def fullSK(lower: Boolean, firstPV: Seq[PhysicalValue[DynamoDBColumn]]) = {
    def asPhysValue[A](m: ColumnMapping[DynamoDBColumn, T, A]) = {
      val a = m.atom
      val (l, r) = a.range
      PhysicalValue(m.name, a, if (lower) l else r)
    }
    val allSK = helper.skColumns.drop(firstPV.length).map(cm => asPhysValue(cm))
    compositeValue("SK", allSK).orElse(allSK.headOption)
  }

  def pkValue(pkv: PKV): PhysicalValue[DynamoDBColumn] = {
    val pkVals = helper.physPkColumns(pkv)
    compositeValue("PK", pkVals).orElse(pkVals.headOption).get
  }

  def skValue(skv: SKV): Option[PhysicalValue[DynamoDBColumn]] = {
    val skVals = helper.physSkColumns(skv)
    compositeValue("SK", skVals).orElse(skVals.headOption)
  }
}

object DynamoDBKeyMapper2 extends Poly1 {
  implicit def byPK[K, T, CR <: HList, KL <: HList, CVL <: HList, PKV]
  (implicit
   columnFamilyHelper: ColumnFamilyHelperBuilder.Aux[DynamoDBColumn, T, CR, CVL, KL, HNil, PKV, HNil])
  = at[(QueryPK[K], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => DynamoTable[K, T, KL, HNil, PKV, HNil](columnFamilyHelper(relation.mapper))
  }

  implicit def queryMulti[K, Cols <: HList, SortCols <: HList,
  T, CR <: HList, KL <: HList, CVL <: HList,
  LeftOverKL <: HList, LeftOverKL2 <: HList, SKL <: HList, PKV, SKV]
  (implicit
   diff1: Diff.Aux[KL, Cols, LeftOverKL],
   diff2: Diff.Aux[LeftOverKL, SortCols, LeftOverKL2],
   prepend: Prepend.Aux[SortCols, LeftOverKL2, SKL],
   columnFamilyHelper: ColumnFamilyHelperBuilder.Aux[DynamoDBColumn, T, CR, CVL, Cols, SKL, PKV, SKV]
  )
  = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => DynamoTable[K, T, Cols, SKL, PKV, SKV](columnFamilyHelper(relation.mapper))
  }

  implicit def writes[K, RD] = at[(RelationWriter[K], RD)](_ => ())
}

object ConvertQueries2 extends Poly3 {

  case class BuilderState[Created, Available, Builders, Writers](tablesCreated: Created, tablesAvailable: Available,
                                                                 builders: Builders, writers: Writers,
                                                                 ddl: Eval[Vector[DynamoDBDDL]], tableNamer: TableNameDetails => String,
                                                                 tableNames: Set[String] = Set.empty
                                                                )

  case class QueryCreate[K, T, PKL, SKL, PKV, SKV, Out](matched: DynamoTable[K, T, PKL, SKL, PKV, SKV], build: String => Out)

  trait mapQueryLP extends Poly1 {
    implicit def nextEntry[Q, RD, H, T <: HList](implicit tailEntry: Case1[mapQuery.type, (Q, RD, T)]) = at[(Q, RD, H :: T)] {
      case (q, rd, l) => tailEntry(q, rd, l.tail)
    }
  }

  object mapQuery extends mapQueryLP {

    implicit def pkQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList,
    KL <: HList, SKL <: HList, Tail <: HList, AllKL <: HList, PKV]
    (implicit
     matchKL: Diff.Aux[KL, PKL, HNil]
    )
    = at[(QueryPK[K], RelationDef[T, CR, PKL, CVL], DynamoTable[K, T, KL, SKL, PKV, HNil] :: Tail)] {
      case (q, rd, table :: _) => QueryCreate[K, T, KL, SKL, PKV, HNil, UniqueQuery[Effect, T, PKV]](table, { tableName =>
        val columns = rd.columns
        def doQuery(v: PKV): Effect[Option[T]] = ReaderT { s =>
          asyncWR(new GetItemRequest(tableName, asAttrMap(Seq(table.pkValue(v)))), s.client.getItemAsync).map {
            (gir: GetItemResult) => Option(gir.getItem()).map(createMaterializer).flatMap(table.helper.materializer)
          }
        }
        UniqueQuery(doQuery)
      })
    }

    implicit def rangeQuery[K, Cols <: HList, SortCols <: HList, Q, T, RD, CR <: HList, CVL <: HList,
    PKL <: HList, KL <: HList, SKL <: HList, Tail <: HList, CRK <: HList,
    SortVals <: HList, SortLen <: Nat, PKV, SKV]
    (implicit
     evSame: Cols =:= KL,
     lenSC: Length.Aux[SortCols, SortLen],
     firstSK: Take.Aux[SKL, SortLen, SortCols],
     skColsLookup: ColumnsAsSeq.Aux[CR, SortCols, T, DynamoDBColumn, SortVals]
    )
    = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, PKL, CVL], DynamoTable[K, T, KL, SKL, PKV, SKV] :: Tail)] {
      case (q, rd, table :: _) => QueryCreate[K, T, KL, SKL, PKV, SKV, RangeQuery[Effect, T, PKV, SortVals]](table, { tn =>
        val (_, sortVals) = skColsLookup(rd.columns)
        def doQuery(c: PKV, lr: RangeValue[SortVals], ur: RangeValue[SortVals], asc: Option[Boolean]): Stream[Effect, T] = {
          def doRange(l: Boolean, i: String, x: String, rv: RangeValue[SortVals]) = {
            rv.fold(i, x).flatMap { case (sv, op) => table.fullSK(l, sortVals(sv)).map((_, op)) }
          }
          val matcher = KeyMatch(table.pkValue(c), doRange(false, ">=", ">", lr), doRange(true, "<=", "<", ur))
          val qr = matcher.forRequest(new QueryRequest(tn))
          val qr2 = asc.map(a => qr.withScanIndexForward(a)).getOrElse(qr)
          resultStream(qr2).map(map => table.helper.materializer(createMaterializer(map))).collect {
            case Some(a) => a
          }
        }
        RangeQuery(None, doQuery)
      })
    }
  }


  trait foldQueriesLP extends Poly2 {
    implicit def buildNewTable[Q, T, CR <: HList, KL <: HList, CVL <: HList, Used <: HList,
    Available, Builders <: HList, Writers <: HList, K, PKL, SKL, Out, AllK <: HList, PKV, SKV]
    (implicit
     mq: Case1.Aux[mapQuery.type, (Q, RelationDef[T, CR, KL, CVL], Available), QueryCreate[K, T, PKL, SKL, PKV, SKV, Out]],
     update: UpdateOrAdd[Writers, K, WriteQueries[Effect, T]]
    )
    = at[(Q, RelationDef[T, CR, KL, CVL]), BuilderState[Used, Available, Builders, Writers]] {
      case ((q, rd), bs) =>
        val cq = mq(q, rd, bs.tablesAvailable)
        val dynamoTable = cq.matched
        val helper = dynamoTable.helper
        val columnsRecord = rd.mapper.columns
        // val (columns, _) = allCols(columnsRecord)
        val skNames = helper.skColumns.map(_.name)
        val pkNames = helper.pkColumns.map(_.name)
        val tableName = bs.tableNamer(TableNameDetails(bs.tableNames, rd.baseName, pkNames, skNames))

        def keysAsAttributes(key: PKV :: SKV :: HNil) = key match {
          case (pkv :: skv :: HNil) => asAttrMap(Seq(dynamoTable.pkValue(pkv)) ++ dynamoTable.skValue(skv))
        }


        def createDDL: CreateTableRequest = {
          val keyColumn = cq.matched.pkAtom
          val sortKeyList = cq.matched.skAtom
          val sortDef = (List(keyColumn) ++ sortKeyList).map(c => new AttributeDefinition(c.name, c.atom.attributeType))
          val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
          val sortKeyDef = sortKeyList.headOption.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
          new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))
        }
        val writer: WriteQueries[Effect, T] = new WriteQueries[Effect, T] {


          def delete(t: T): Effect[Unit] = ReaderT { s =>
            asyncWR(new DeleteItemRequest(tableName, keysAsAttributes(helper.extractKey(t))), s.client.deleteItemAsync).map(_ => ())
          }

          def insert(t: T): Effect[Unit] = ReaderT { s =>
            asyncWR(new PutItemRequest(tableName, asAttrMap(dynamoTable.toPhysicalValues(t))), s.client.putItemAsync).map(_ => ())
          }

          def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
            helper.changeChecker(existing, newValue).map {
              case Xor.Left((k, changes)) =>
                asyncWR(new UpdateItemRequest(tableName, keysAsAttributes(k), changes.map(c => asValueUpdate(c)).toMap.asJava),
                  s.client.updateItemAsync)
              case Xor.Right((oldKey, newk, vals)) =>
                asyncWR(new DeleteItemRequest(tableName, keysAsAttributes(oldKey)), s.client.deleteItemAsync) <*
                  asyncWR(new PutItemRequest(tableName, asAttrMap(vals)), s.client.putItemAsync)
            } map (_.map(_ => true)) getOrElse Task.now(false)
          }
        }
        bs.copy[DynamoTable[K, T, PKL, SKL, PKV, SKV] :: Used, Available, Out :: Builders, update.Out](
          tablesCreated = dynamoTable.copy[K, T, PKL, SKL, PKV, SKV](name = tableName) :: bs.tablesCreated,
          builders = cq.build(tableName) :: bs.builders,
          writers = update(bs.writers, ex => ex.map(x => WriteQueries.combine(x, writer)).getOrElse(writer)),
          ddl = bs.ddl.map(_ :+ createDDL),
          tableNames = bs.tableNames + tableName
        )
    }
  }

  object foldQueries extends foldQueriesLP {
    implicit def writeQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList, Used, Available, Builders <: HList, Writers <: HList]
    = at[(RelationWriter[K], RelationDef[T, CR, PKL, CVL]), BuilderState[Used, Available, Builders, Writers]] {
      case ((q, rd), bs) => bs.copy[Used, Available, RelationWriter[K] :: Builders, Writers](builders = q :: bs.builders)
    }

    implicit def existingTable[Q, RD, Used, Available, Builders <: HList, Writers <: HList, K, T, KL, SKL, PKV, SKV, Out]
    (implicit mq: Case1.Aux[mapQuery.type, (Q, RD, Used), QueryCreate[K, T, KL, SKL, PKV, SKV, Out]])
    = at[(Q, RD), BuilderState[Used, Available, Builders, Writers]] {
      case ((q, rd), bs) =>
        val cq = mq(q, rd, bs.tablesCreated)
        bs.copy[Used, Available, Out :: Builders, Writers](builders = cq.build(cq.matched.name) :: bs.builders)
    }
  }


  implicit def convertAll[Q <: HList, Tables <: HList, WithSK <: HList, NoSK <: HList,
  SortedTables <: HList, QueriesAndTables <: HList, Created, OutQueries <: HList, Writers]
  (implicit
   collect: Collect.Aux[Tables, tablesWithSK.type, WithSK],
   collect2: Collect.Aux[Tables, tablesNoSK.type, NoSK],
   prepend: Prepend.Aux[WithSK, NoSK, SortedTables],
   folder: RightFolder.Aux[Q, BuilderState[HNil, SortedTables, HNil, HNil],
     foldQueries.type, BuilderState[Created, SortedTables, OutQueries, Writers]],
   finishUp: MapWith[Writers, OutQueries, finishWrites.type]
  ) = at[Q, Tables, SimpleMapperConfig] { (q, tables, config) =>
    val folded = folder(q, BuilderState(HNil, prepend(collect(tables), collect2(tables)), HNil, HNil, Eval.now(Vector.empty), config.tableNamer))
    val outQueries = finishUp(folded.writers, folded.builders)
    BuiltQueries[finishUp.Out, DynamoDBDDL](outQueries, folded.ddl.map(a => a))
  }

  trait finishWritesLP extends Poly2 {
    implicit def any[A, B] = at[A, B]((a, b) => b)
  }

  object finishWrites extends finishWritesLP {
    implicit def getWriter[K, W <: HList](implicit s: Selector[W, K]) = at[W, RelationWriter[K]] { case (w, _) => s(w) }
  }

}