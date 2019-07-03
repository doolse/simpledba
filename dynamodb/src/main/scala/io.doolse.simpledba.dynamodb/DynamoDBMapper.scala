package io.doolse.simpledba.dynamodb

import io.doolse.simpledba._
import shapeless.labelled.KeyTag
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, LabelledGeneric, Witness}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, KeyType, Projection}

class RelationBuilder[T] {

  def embedded[GR <: HList, R <: HList](
      implicit
      gen: LabelledGeneric.Aux[T, GR],
      columns: ColumnBuilder.Aux[DynamoDBColumn, GR, R]
  ): ColumnBuilder.Aux[DynamoDBColumn, T, R] = new ColumnBuilder[DynamoDBColumn, T] {
    type Repr = R

    def apply() = columns().compose(Iso(gen.to, gen.from))
  }

  def table[TR <: HList, CR <: HList](name: String)(
      implicit lgen: LabelledGeneric.Aux[T, TR],
      columns: ColumnBuilder.Aux[DynamoDBColumn, TR, CR]): TableBuilder[T, TR, CR] = {
    TableBuilder[T, TR, CR](name, lgen, columns())
  }
}

case class TableBuilder[T, TRec, ColRec <: HList](name: String,
                                                  generic: LabelledGeneric.Aux[T, TRec],
                                                  columns: Columns[DynamoDBColumn, TRec, ColRec]) {

  def partKey[Out <: HList, PK](col: Witness)(
      implicit
      ev: col.T <:< Symbol,
      pkSubset: ColumnSubsetBuilder.Aux[ColRec, col.T :: HNil, Out],
      ev2: Out <:< (PK :: HNil),
      isPK: DynamoDBPKColumn[PK]): PartKeyTable[T, ColRec, PK, HNil] = {
    val toOut  = columns.subset(pkSubset).from
    val pkName = col.value.name
    val pkCol  = KeyAttribute.unsafe[PK](KeyType.HASH, columns.columns.find(_._1 == pkName).get)
    PartKeyTable[T, ColRec, PK, HNil](name,
                                      pkCol,
                                      columns.compose(Iso(generic.to, generic.from)),
                                      t => columns.iso.to(generic.to(t)),
                                      toOut.andThen(_.head),
                                      (_, _) => Seq(),
                                      Seq.empty)
  }

  def partKeys[Out <: HList, PKL <: HList](cols: Cols[PKL])(
      implicit
      pkSubset: ColumnSubsetBuilder.Aux[ColRec, PKL, Out],
      keyBuilder: BinaryKey[Out]): PartKeyTable[T, ColRec, Out, HNil] = {
    val toOut = columns.subset(pkSubset).from

    val derivedKey = KeyAttribute
      .mapped[SdkBytes, Out](KeyType.HASH,
                             DynamoDBMapper.derivedPK.value.name,
                             DynamoDBColumn.bytesCol,
                             BinaryKey.apply[Out])
    PartKeyTable(
      name,
      derivedKey,
      columns.compose(Iso(a => generic.to(a), generic.from)),
      t => columns.iso.to(generic.to(t)),
      toOut,
      (_, rec) => Seq(derivedKey.toNamedValue(toOut(rec))),
      Seq.empty
    )
  }

}

case class PartKeyTable[T0, ColRec <: HList, PK0, Indexes0 <: HList](
    name: String,
    pkColumn: KeyAttribute[PK0],
    columns: Columns[DynamoDBColumn, T0, ColRec],
    toRec: T0 => ColRec,
    pkValue: ColRec => PK0,
    derivedColumns: (T0, ColRec) => Seq[(String, AttributeValue)],
    localIndexes: Seq[LocalIndex[_]])
    extends DynamoDBTable {
  type T       = T0
  type CR      = ColRec
  type Indexes = Indexes0
  type PK      = PK0
  type FullKey = PK0

  val keyColumns = Seq(pkColumn)

  override def keyValue: T0 => PK0 = t => pkValue(toRec(t))

  override def keyAttributes: PK0 => Seq[(String, AttributeValue)] =
    pk => Seq(pkColumn.toNamedValue(pk))

  def sortKey[SK, Out <: HList](col: Witness)(
      implicit
      ev: col.T <:< Symbol,
      skSubset: ColumnSubsetBuilder.Aux[ColRec, col.T :: HNil, Out],
      ev2: Out <:< (SK :: HNil),
      isPK: DynamoDBPKColumn[SK]): FullKeyTable[T0, ColRec, PK0, SK, Indexes0] = {
    val toOut  = columns.subset(skSubset).from
    val skName = col.value.name
    val skCol  = KeyAttribute.unsafe[SK](KeyType.RANGE, columns.columns.find(_._1 == skName).get)
    FullKeyTable(name, pkColumn, skCol, columns, t => {
      val rec = toRec(t)
      HList(pkValue(rec), toOut(rec).head)
    }, derivedColumns, localIndexes)
  }

  def derivedSortKey[A](f: T0 => A, attrName: String = "derivedSK")(
      implicit ev: DynamoDBPKColumn[A],
      col: DynamoDBColumn[A]): FullKeyTable[T0, ColRec, PK0, A, Indexes0] = {
    val skCol = KeyAttribute(KeyType.RANGE, (attrName, col))
    FullKeyTable(name, pkColumn, skCol, columns, t => {
      HList(pkValue(toRec(t)), f(t))
    }, (t, rec) => derivedColumns(t, rec) :+ skCol.toNamedValue(f(t)), localIndexes)
  }

  def sortKeys[Out <: HList, SKL <: HList](cols: Cols[SKL])(
      implicit
      skSubset: ColumnSubsetBuilder.Aux[ColRec, SKL, Out],
      keyBuilder: BinaryKey[Out]): FullKeyTable[T0, ColRec, PK0, Out, Indexes0] = {
    val toOut = columns.subset(skSubset).from

    val derivedKey = KeyAttribute
      .mapped[SdkBytes, Out](KeyType.RANGE,
                             DynamoDBMapper.derivedSK.value.name,
                             DynamoDBColumn.bytesCol,
                             BinaryKey.apply[Out])
    FullKeyTable(
      name,
      pkColumn,
      derivedKey,
      columns,
      t => {
        val rec = toRec(t)
        HList(pkValue(rec), toOut(rec))
      },
      (t, rec) => derivedColumns(t, rec) :+ derivedKey.toNamedValue(toOut(rec)),
      localIndexes
    )
  }

}

case class FullKeyTable[T0, ColRec <: HList, PK0, SK0, Indexes0 <: HList](
    name: String,
    pkColumn: KeyAttribute[PK0],
    skColumn: KeyAttribute[SK0],
    columns: Columns[DynamoDBColumn, T0, ColRec],
    keyValue: T0 => PK0 :: SK0 :: HNil,
    derivedColumns: (T0, ColRec) => Seq[(String, AttributeValue)],
    localIndexes: Seq[LocalIndex[_]])
    extends DynamoDBSortTable {
  type T       = T0
  type CR      = ColRec
  type Indexes = Indexes0
  type PK      = PK0
  type SK      = SK0

  val keyColumns = Seq(pkColumn, skColumn)

  override def keyAttributes: FullKey => Seq[(String, AttributeValue)] =
    fk => Seq(pkColumn.toNamedValue(fk.head), skColumn.toNamedValue(fk.tail.head))

  def withLocalIndex[IK](name: Witness, column: Witness)(
      implicit kc: Selector.Aux[ColRec, column.T, IK],
      dynamoDBColumn: DynamoDBColumn[IK],
      ev: name.T <:< Symbol,
      ev2: column.T <:< Symbol)
    : FullKeyTable[T0, ColRec, PK0, SK0, IK with KeyTag[name.T, IK] :: Indexes0] =
    copy(
      localIndexes = localIndexes :+
        LocalIndex(name.value.name,
                   KeyAttribute(KeyType.RANGE, (column.value.name, dynamoDBColumn)),
                   Projection.builder().projectionType("ALL").build))
}

object DynamoDBMapper {
  val derivedPK = Witness('derivedPK)
  val derivedSK = Witness('derivedSK)
}

class DynamoDBMapper[S[_], F[_]](effect: DynamoDBEffect[S, F]) {

  def mapped[T] = new RelationBuilder[T]

  def flush(writes: S[DynamoDBWriteOp]): F[Unit] = {
    val S = effect.S
    val M = S.SM
    S.drain {
      M.flatMap(S.eval(effect.asyncClient)) { client =>
        S.evalMap(writes) {
          case PutItem(request) =>
            effect.void(effect.fromFuture(client.putItem(request)))
          case DeleteItem(request) =>
            effect.void(effect.fromFuture(client.deleteItem(request)))
        }
      }
    }
  }

  val queries: DynamoDBQueries[S, F] = new DynamoDBQueries[S, F](effect)
}
