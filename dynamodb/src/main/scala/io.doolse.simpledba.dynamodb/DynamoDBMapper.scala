package io.doolse.simpledba.dynamodb

import fs2._
import io.doolse.simpledba.{ColumnBuilder, Flushable, Iso, WriteOp}
import shapeless.ops.record.Selector
import shapeless.{HList, HNil, LabelledGeneric, Witness}

class DynamoDBMapper[F[_]](effect: DynamoDBEffect[F]) {

  def mapped[T] = new RelationBuilder[T]

  class RelationBuilder[T] {

    def embedded[GR <: HList, R <: HList](
        implicit
        gen: LabelledGeneric.Aux[T, GR],
        columns: ColumnBuilder.Aux[DynamoDBColumn, GR, R]
    ): ColumnBuilder.Aux[DynamoDBColumn, T, R] = new ColumnBuilder[DynamoDBColumn, T] {
      type Repr = R
      def apply() = columns().compose(Iso(gen.to, gen.from))
    }

    def table[CR <: HList, Vals <: HList, PK](name: String, pk: Witness)(
        implicit lgen: LabelledGeneric.Aux[T, CR],
        columns: ColumnBuilder.Aux[DynamoDBColumn, CR, Vals],
        pkCol: Selector.Aux[CR, pk.T, PK],
        ev: pk.T <:< Symbol,
        isPK: DynamoDBPKColumn[PK]): DynamoDBTable.Aux[T, CR, Vals, PK, Unit, HNil] = {
      val cols   = columns.apply()
      val pkName = pk.value.name
      val pkCol  = NamedAttribute.unsafe[PK](cols.columns.find(_._1 == pkName).get)
      DynamoDBTableRepr[T, CR, Vals, PK, Unit, HNil](name,
                                                        pkCol,
                                                        None,
                                                        columns.apply(),
                                                        Iso(lgen.to, lgen.from),
                                                        Seq.empty)
    }

    def table[CR <: HList, Vals <: HList, PK, SK](
        name: String,
        pk: Witness,
        sk: Witness)(
        implicit lgen: LabelledGeneric.Aux[T, CR],
        columns: ColumnBuilder.Aux[DynamoDBColumn, CR, Vals],
        pkCol: Selector.Aux[CR, pk.T, PK],
        skCol: Selector.Aux[CR, sk.T, SK],
        evpk: pk.T <:< Symbol,
        evsk: sk.T <:< Symbol,
        isPK: DynamoDBPKColumn[PK],
        isSK: DynamoDBPKColumn[SK]): DynamoDBTable.Aux[T, CR, Vals, PK, SK, HNil] = {
      val cols   = columns.apply()
      val pkName = pk.value.name
      val skName = sk.value.name
      val pkCol  = NamedAttribute.unsafe[PK](cols.columns.find(_._1 == pkName).get)
      val skCol  = cols.columns.find(_._1 == skName).map(NamedAttribute.unsafe[SK])
      DynamoDBTableRepr[T, CR, Vals, PK, SK, HNil](name,
                                                   pkCol,
                                                   skCol,
                                                   columns.apply(),
                                                   Iso(lgen.to, lgen.from),
                                                   Seq.empty)
    }
  }

  def flusher: Flushable[F] = new Flushable[F] {
    override def flush: Pipe[F, WriteOp, Unit] =
      writes => {
        Stream
          .eval(effect.asyncClient)
          .flatMap { client =>
            writes.evalMap {
              case PutItem(request) =>
                effect.fromFuture(client.putItem(request))
            }
          }
          .drain ++ Stream.emit()
      }
  }

  def queries: DynamoDBQueries[F] = new DynamoDBQueries[F](effect)
}
