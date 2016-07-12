package io.doolse.simpledba.dynamodb

import cats.data.{ReaderT, Xor}
import com.amazonaws.services.dynamodbv2.model.{Stream => _, _}
import fs2._
import fs2.interop.cats._
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.RelationMapper._
import io.doolse.simpledba._
import io.doolse.simpledba.dynamodb.DynamoDBIO._
import io.doolse.simpledba.dynamodb.DynamoDBMapper._
import shapeless._
import cats.syntax.all._
import shapeless.ops.hlist.{Diff, Prepend}

import scala.collection.JavaConverters._

/**
  * Created by jolz on 12/05/16.
  */


object DynamoDBMapper {
  type Effect[A] = ReaderT[Task, DynamoDBSession, A]
  type DynamoDBDDL = CreateTableRequest


  def asAttrMap(l: Seq[PhysicalValue[DynamoDBColumn]]) = l.map(physical2Attribute).toMap.asJava

  def physical2Attribute(pc: PhysicalValue[DynamoDBColumn]) = pc.name -> pc.atom.to(pc.v)

  def asValueUpdate(d: ValueDifference[DynamoDBColumn]) = {
    d.name -> d.atom.diff(d.existing, d.newValue)
  }

  def createMaterializer(m: java.util.Map[String, AttributeValue]) = new ColumnMaterialzer[DynamoDBColumn] {
    def apply[A](name: String, atom: DynamoDBColumn[A]): Option[A] = {
      Option(m.get(name)).map(av => atom.from(av))
    }
  }

  def resultStream(qr: QueryRequest): Stream[Effect, java.util.Map[String, AttributeValue]] = {
    Stream.eval[Effect, QueryResult](ReaderT {
      _.request(queryAsync, qr)
    }).flatMap { result =>
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
  type KeyMapperPoly = DynamoDBKeyMapper.type
  type QueriesPoly = DynamoDBQueries.type

  val stdColumnMaker = new MappingCreator[DynamoDBColumn] {
    def wrapAtom[S, A](atom: DynamoDBColumn[A], to: (S) => A, from: (A) => S): DynamoDBColumn[S] = {
      val (lr, hr) = atom.range
      DynamoDBColumn[S](
        atom.from andThen from,
        atom.to compose to,
        atom.attributeType, (ex, nv) => atom.diff(to(ex), to(nv)), atom.sortablePart compose to, (from(lr), from(hr)))
    }
  }
}

trait DynamoTable[T] extends KeyBasedTable {
  def writer(name: String): WriteQueries[Effect, T]

  def createDDL(name: String): CreateTableRequest

  def pkNames: Seq[String]

  def skNames: Seq[String]

  def materializer: ColumnMaterialzer[DynamoDBColumn] => Option[T]

  def realPK(logical: Seq[PhysicalValue[DynamoDBColumn]]): PhysicalValue[DynamoDBColumn]

  def fullSK(lower: Boolean, firstPV: Seq[PhysicalValue[DynamoDBColumn]]): Option[PhysicalValue[DynamoDBColumn]]
}

trait DynamoTableBuilder[T, CR <: HList, CVL <: HList, PKL, SKL] {
  def apply(mapper: ColumnMapper[T, CR, CVL]): DynamoTable[T]
}

object DynamoTableBuilder {
  implicit def makeTable[CR <: HList, AllK <: HList, T, PKL, SKL, CVL <: HList, PKV, SKV]
  (implicit helperB: ColumnFamilyHelperBuilder.Aux[DynamoDBColumn, T, CR, CVL, PKL, SKL, PKV, SKV])
  = new DynamoTableBuilder[T, CR, CVL, PKL, SKL] {

    def apply(mapper: ColumnMapper[T, CR, CVL]) = new DynamoTable[T] {
      val SK = "SK"
      val PK = "PK"

      val helper = helperB(mapper)

      def toPhysicalValues(t: T): Seq[PhysicalValue[DynamoDBColumn]] = {
        val (all, pk, sk) = helper.toAllPhysicalValues(t)
        all ++ compositeValue(PK, pk) ++ compositeValue(SK, sk)
      }


      def compositeValue(prefix: String, vals: Seq[PhysicalValue[DynamoDBColumn]]): Option[PhysicalValue[DynamoDBColumn]] = if (vals.length > 1) {
        Some(PhysicalValue(prefix + "_Composite", DynamoDBColumn.stringColumn, vals.map(pv => pv.atom.sortablePart(pv.v)).mkString(",")))
      } else None


      def writer(tableName: String) = new WriteQueries[Effect, T] {

        def extraFields(key: PKV :: SKV :: HNil): Seq[PhysicalValue[DynamoDBColumn]] = compositeValue(PK, helper.physPkColumns(key.head)).toSeq ++
          compositeValue(SK, helper.physSkColumns(key.tail.head))

        def delete(t: T): Effect[Unit] = ReaderT {
          _.request(deleteItemAsync, new DeleteItemRequest(tableName, keysAsAttributes(helper.extractKey(t)))).map(_ => ())
        }

        def insert(t: T): Effect[Unit] = ReaderT {
          _.request(putItemAsync, new PutItemRequest(tableName, asAttrMap(toPhysicalValues(t)))).map(_ => ())
        }

        def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
          helper.changeChecker(existing, newValue).map {
            case Xor.Left((k, changes)) =>
              s.request(updateItemAsync, new UpdateItemRequest(tableName, keysAsAttributes(k),
                changes.map(c => asValueUpdate(c)).toMap.asJava))
            case Xor.Right((oldKey, newk, vals)) =>
              s.request(deleteItemAsync, new DeleteItemRequest(tableName, keysAsAttributes(oldKey))) <*
                s.request(putItemAsync, new PutItemRequest(tableName, asAttrMap(vals ++ extraFields(newk))))
          } map (_.map(_ => true)) getOrElse Task.now(false)
        }
      }


      def createDDL(tableName: String): DynamoDBDDL = {
        def atomForKey(prefix: String, cols: Seq[ColumnMapping[DynamoDBColumn, T, _]]) = if (cols.length > 1)
          Some(ColumnMapping[DynamoDBColumn, T, String](prefix + "_Composite", DynamoDBColumn.stringColumn, _ => ???))
        else cols.headOption

        lazy val pkAtom = atomForKey(PK, helper.pkColumns).get
        lazy val skAtom = atomForKey(SK, helper.skColumns)


        val keyColumn = pkAtom
        val sortKeyList = skAtom
        val sortDef = (List(keyColumn) ++ sortKeyList).map(c => new AttributeDefinition(c.name, c.atom.attributeType))
        val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
        val sortKeyDef = sortKeyList.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
        new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))

      }

      val pkNames: Seq[String] = helper.pkColumns.map(_.name)

      val skNames: Seq[String] = helper.skColumns.map(_.name)

      def materializer: (ColumnMaterialzer[DynamoDBColumn]) => Option[T] = helper.materializer

      def realPK(logical: Seq[PhysicalValue[DynamoDBColumn]]): PhysicalValue[DynamoDBColumn] =
        compositeValue(PK, logical).orElse(logical.headOption).get

      def fullSK(lower: Boolean, firstPV: Seq[PhysicalValue[DynamoDBColumn]]) = {
        def asPhysValue[A](m: ColumnMapping[DynamoDBColumn, T, A]) = {
          val a = m.atom
          val (l, r) = a.range
          PhysicalValue(m.name, a, if (lower) r else l)
        }
        val allSK = firstPV ++ helper.skColumns.drop(firstPV.length).map(cm => asPhysValue(cm))
        compositeValue(SK, allSK).orElse(allSK.headOption)
      }

      def pkValue(pkv: PKV): PhysicalValue[DynamoDBColumn] = {
        realPK(helper.physPkColumns(pkv))
      }

      def skValue(skv: SKV): Option[PhysicalValue[DynamoDBColumn]] = {
        val skVals = helper.physSkColumns(skv)
        compositeValue(SK, skVals).orElse(skVals.headOption)
      }

      def keysAsAttributes(key: PKV :: SKV :: HNil) = key match {
        case (pkv :: skv :: HNil) => asAttrMap(Seq(pkValue(pkv)) ++ skValue(skv))
      }

    }
  }
}

object DynamoDBKeyMapper extends Poly1 {
    implicit def byPK[K, T, CR <: HList, KL <: HList, CVL <: HList, PKV]
    (implicit
     builder: DynamoTableBuilder[T, CR, CVL, KL, HNil])
    = at[(QueryPK[K], RelationDef[T, CR, KL, CVL])] {
      case (q, relation) => (relation.baseName, builder(relation.mapper))
    }

    implicit def queryMulti[K, Cols <: HList, SortCols <: HList,
    T, CR <: HList, KL <: HList, CVL <: HList,
    LeftOverKL <: HList, LeftOverKL2 <: HList, SKL <: HList, PKV, SKV]
    (implicit
     diff1: Diff.Aux[KL, Cols, LeftOverKL],
     diff2: Diff.Aux[LeftOverKL, SortCols, LeftOverKL2],
     prepend: Prepend.Aux[SortCols, LeftOverKL2, SKL],
     builder: DynamoTableBuilder[T, CR, CVL, Cols, SKL]
    )
    = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, KL, CVL])] {
      case (q, relation) => (relation.baseName, builder(relation.mapper))
    }

    implicit def writes[K, RD] = at[(RelationWriter[K], RD)](_ => ())
}

object mapQuery extends Poly2 {

  implicit def pkQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList, PKV]
  (implicit
   lookupPK: ColumnsAsSeq.Aux[CR, PKL, T, DynamoDBColumn, PKV]
  )
  = at[QueryPK[K], RelationDef[T, CR, PKL, CVL]] { (q, rd) =>
    val (pkCols, pkVals) = lookupPK(rd.columns)
    val pkNames = pkCols.map(_.name)
    def pkMatch[T](table: DynamoTable[T]) = table.pkNames == pkNames
    QueryCreate[DynamoTable[T], UniqueQuery[Effect, T, PKV]](pkMatch, { (tableName, dt) =>
      val columns = rd.columns
      val scanAll = resultStream(new QueryRequest(tableName)).map(createMaterializer).map(dt.materializer).collect {
        case Some(a) => a
      }
      def doQuery(v: PKV): Effect[Option[T]] = ReaderT { s =>
        s.request(getItemAsync, new GetItemRequest(tableName, asAttrMap(Seq(dt.realPK(pkVals(v)))))).map {
          gir => Option(gir.getItem).map(createMaterializer).flatMap(dt.materializer)
        }
      }
      UniqueQuery[Effect, T, PKV](doQuery, scanAll)
    })
  }

  implicit def rangeQuery[K, Cols <: HList, SortCols <: HList, Q, T, RD, CR <: HList, CVL <: HList,
  PKL <: HList, KL <: HList, SKL <: HList, SortVals <: HList, PKV, SKV]
  (implicit
   pkColsLookup: ColumnsAsSeq.Aux[CR, Cols, T, DynamoDBColumn, PKV],
   skColsLookup: ColumnsAsSeq.Aux[CR, SortCols, T, DynamoDBColumn, SortVals]
  )
  = at[QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, PKL, CVL]] { (q, rd) =>
    val (pkCols, pkVals) = pkColsLookup(rd.columns)
    val (skCols, sortVals) = skColsLookup(rd.columns)
    val pkNames = pkCols.map(_.name)
    val skNames = skCols.map(_.name)
    def multiMatch[T](table: DynamoTable[T]) = {
      table.pkNames == pkNames && table.skNames.startsWith(skNames)
    }
    QueryCreate[DynamoTable[T], RangeQuery[Effect, T, PKV, SortVals]](multiMatch, { (tn, table) =>
      def doQuery(c: PKV, lr: RangeValue[SortVals], ur: RangeValue[SortVals], asc: Option[Boolean]): Stream[Effect, T] = {
        def doRange(l: Boolean, i: String, x: String, rv: RangeValue[SortVals]) = {
          rv.fold((i,false), (x,true)).flatMap { case (sv, (op,x)) => table.fullSK(l^x, sortVals(sv)).map((_, op)) }
        }
        val matcher = KeyMatch(table.realPK(pkVals(c)), doRange(false, ">=", ">", lr), doRange(true, "<=", "<", ur))
        val qr = matcher.forRequest(new QueryRequest(tn))
        val qr2 = asc.map(a => qr.withScanIndexForward(a)).getOrElse(qr)
        resultStream(qr2).map(map => table.materializer(createMaterializer(map))).collect {
          case Some(a) => a
        }
      }
      RangeQuery(None, doQuery)
    })
  }
}


object DynamoDBQueries extends QueryFolder[Effect, DynamoDBDDL, DynamoTable, mapQuery.type] {

  def createWriter[T](v: Vector[(String, DynamoTable[T])]): WriteQueries[Effect, T] = v.map {
    case (name, table) => table.writer(name)
  }.reduce(WriteQueries.combine[Effect, T])

  def createTableDDL[T](s: String, table: DynamoTable[T]) = table.createDDL(s)

}