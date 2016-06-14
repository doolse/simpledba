package io.doolse.simpledba.dynamodb

import cats.{Applicative, Monad}
import cats.data.{Reader, ReaderT, Xor}
import cats.syntax.all._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import fs2.interop.cats._
import fs2._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{Stream => _, _}
import fs2.util.{Catchable, Task}
import io.doolse.simpledba._
import io.doolse.simpledba.dynamodb.DynamoDBMapper._
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist
import shapeless.ops.hlist.{Intersection, Prepend, RemoveAll, ToList}
import shapeless.ops.record.{SelectAll, Selector}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 12/05/16.
  */

case class DynamoDBSession(client: AmazonDynamoDBAsync)

object DynamoDBMapper {
  type Effect[A] = ReaderT[Task, DynamoDBSession, A]


  def asAttrMap(l: List[PhysicalValue[DynamoDBColumn]]) = l.map(physical2Attribute).toMap.asJava

  def physical2Attribute(pc: PhysicalValue[DynamoDBColumn]) = pc.name -> pc.atom.to(pc.v)
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

class DynamoDBMapper extends RelationMapper[DynamoDBMapper.Effect] {

  type ColumnAtom[A] = DynamoDBColumn[A]
  type DDLStatement = CreateTableRequest
  type KeyMapperT = DynamoDBKeyMapper

  val stdColumnMaker = new MappingCreator[DynamoDBColumn] {
    def wrapAtom[S, A](atom: DynamoDBColumn[A], to: (S) => A, from: (A) => S): DynamoDBColumn[S] = {
      val (lr, hr) = atom.range
      DynamoDBColumn[S](
        atom.from andThen from,
        atom.to compose to,
        atom.attributeType, (ex, nv) => atom.diff(to(ex), to(nv)), atom.compositePart compose to, (from(lr), from(hr)))
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

  implicit val strat = Strategy.fromExecutionContext(ExecutionContext.global)

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
      = KeyMatch(pkVals(convPK(pk)), skVals(convSK(sk, true)).headOption.map((_, "=")), None)

      def asValueUpdate(d: ValueDifference[DynamoDBColumn]) = {
        d.name -> d.atom.diff(d.existing, d.newValue)
      }

      def createMaterializer(m: java.util.Map[String, AttributeValue]) = new ColumnMaterialzer[DynamoDBColumn] {
        def apply[A](name: String, atom: DynamoDBColumn[A]): Option[A] = {
          Option(m.get(name)).map(av => atom.from(av))
        }
      }

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

      def async[R <: AmazonWebServiceRequest, A](r: R, f: (R, AsyncHandler[R, A]) => java.util.concurrent.Future[A]): Task[A] = {
        Task.async[A] {
          cb => f(r, new AsyncHandler[R, A] {
            def onError(exception: Exception): Unit = cb(Left(exception))
            def onSuccess(request: R, result: A): Unit = cb(Right(result))
          })
        }
      }

      def createReadQueries: ReadQueries = new ReadQueries {
        def selectOne[A](projection: DynamoProjection[A], where: DynamoWhere): Effect[Option[A]] = ReaderT { s =>
          async(new GetItemRequest(tableName, where.asMap), s.client.getItemAsync).map {
            (gir:GetItemResult) => Option(gir.getItem()).map(createMaterializer).flatMap(projection.materialize)
          }
        }


        def doQuery(qr: QueryRequest): Effect[QueryResult] = ReaderT { s => async(qr, s.client.queryAsync) }

        def resultStream(qr: QueryRequest) : Stream[Effect, java.util.Map[String, AttributeValue]] = {
          val request = Stream.eval[Effect, QueryResult](ReaderT(s => async(qr, s.client.queryAsync)))
          request.flatMap {
            result =>
              val chunk = Stream.chunk(Chunk.seq(result.getItems.asScala))
              if (result.getLastEvaluatedKey == null) chunk
              else chunk ++ resultStream(qr.withExclusiveStartKey(result.getLastEvaluatedKey))
          }
        }

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
          async(new DeleteItemRequest(tableName, keysAsAttributes(toKeys(colMapper.toColumns(t)))), s.client.deleteItemAsync).map(_ => ())
        }

        def insert(t: T): Effect[Unit] = ReaderT { s =>
          async(new PutItemRequest(tableName, asAttrMap(helper.toPhysicalValues(t))), s.client.putItemAsync).map(_ => ())
        }

        def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
          helper.changeChecker(existing, newValue).map {
            case Xor.Left((k, changes)) =>
              async(new UpdateItemRequest(tableName, keysAsAttributes(k), changes.map(c => asValueUpdate(c)).toMap.asJava), s.client.updateItemAsync)
            case Xor.Right((oldKey, newk, vals)) =>
              async(new DeleteItemRequest(tableName, keysAsAttributes(oldKey)), s.client.deleteItemAsync) <*
                async(new PutItemRequest(tableName, asAttrMap(vals)), s.client.putItemAsync)
          } map ( _.map(_ => true)) getOrElse Task.now(false)
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
      val asString: PhysicalValue[DynamoDBColumn] => String = db => db.atom.compositePart(db.v)
      def combine(l: List[PhysicalValue[DynamoDBColumn]]) = l.map(asString).mkString(",")

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
  ): Aux[T, CR, PKK :: HNil, CVL, QueryUnique[K, HNil], PKK, HNil, PKV, HNil]
  = DynamoDBKeyMapper(relMaker)

  implicit def twoKeys[K, T, CR <: HList, CVL <: HList, PKK, PKV, SKK, SKV, PKC, SKC]
  (implicit
   selPK: Selector.Aux[CR, PKK, PKC],
   cv: ColumnValues.Aux[PKC, PKV],
   selSK: Selector.Aux[CR, SKK, SKC],
   cvSK: ColumnValues.Aux[SKC, SKV],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV :: HNil, PKV, SKV :: HNil]
  ): Aux[T, CR, PKK :: SKK :: HNil, CVL, QueryUnique[K, HNil], PKK, SKK :: HNil, PKV, SKV :: HNil]
  = DynamoDBKeyMapper(relMaker)

  implicit def multiNoSort[K, T, CR <: HList, CVL <: HList, KL <: HList, PKK, PKV, SKL, SKV]
  (implicit
   remPK: hlist.Remove.Aux[KL, PKK, (PKK, SKL)],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKL, PKV, SKV, PKV, SKV]
  ): Aux[T, CR, KL, CVL, QueryMultiple[K, PKK :: HNil, HNil], PKK, SKL, PKV, SKV]
  = DynamoDBKeyMapper(relMaker)
}
