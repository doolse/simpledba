package io.doolse.simpledba.dynamodb

import fs2._
import io.doolse.simpledba.{ColumnBuilder, Flushable, Iso, WriteOp}
import shapeless.ops.record.Selector
import shapeless.{HList, LabelledGeneric, Witness}

class DynamoDBMapper[F[_]](effect: DynamoDBEffect[F]) {

  def mapped[T] = new RelationBuilder[T]

  class RelationBuilder[T] {
    def table[CR <: HList, Vals <: HList, PKS <: Symbol, PK](name: String, pk: Witness.Aux[PKS])(
        implicit lgen: LabelledGeneric.Aux[T, CR],
        columns: ColumnBuilder.Aux[DynamoDBColumn, CR, Vals],
        pkCol: Selector.Aux[CR, PKS, PK],
        isPK: DynamoDBPKColumn[PK]) = {
      val cols   = columns.apply()
      val pkName = pk.value.name
      val pkCol  = cols.columns.find(_._1 == pkName).get
      DynamoDBTable[T, CR, Vals, PK, Nothing](name,
                                              pkCol.asInstanceOf[(String, DynamoDBColumn[PK])],
                                              None,
                                              columns.apply(),
                                              Iso(lgen.to, lgen.from))
    }
  }

  def flusher: Flushable[F] = new Flushable[F] {
    override def flush: Pipe[F, WriteOp, Unit] =
      writes =>
        Stream
          .eval(effect.asyncClient)
          .flatMap { client =>
            writes.evalMap {
              case PutItem(request) => effect.fromFuture(client.putItem(request))
            }
          }
          .drain
  }

  def queries: DynamoDBQueries[F] = new DynamoDBQueries[F]
}
