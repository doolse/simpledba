package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.{ColumnBuilder, Flushable, Iso, WriteOp}
import shapeless.ops.record.Selector
import shapeless.{HList, HNil, LabelledGeneric, Witness}

class DynamoDBMapper[S[_], F[_]](effect: DynamoDBEffect[S, F]) {

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

    def table[TR <: HList, CR <: HList, PK](name: String, pk: Witness)(
        implicit lgen: LabelledGeneric.Aux[T, TR],
        columns: ColumnBuilder.Aux[DynamoDBColumn, TR, CR],
        pkCol: Selector.Aux[CR, pk.T, PK],
        ev: pk.T <:< Symbol,
        isPK: DynamoDBPKColumn[PK]): DynamoDBTable.Aux[T, CR, PK, Unit, HNil] = {
      val cols   = columns()
      val pkName = pk.value.name
      val pkCol  = NamedAttribute.unsafe[PK](cols.columns.find(_._1 == pkName).get)
      DynamoDBTableRepr[T, CR, PK, Unit, HNil](name,
                                               pkCol,
                                               None,
                                               cols.compose(Iso(lgen.to, lgen.from)),
                                               Seq.empty)
    }

    def table[TR <: HList, CR <: HList, PK, SK](name: String, pk: Witness, sk: Witness)(
        implicit lgen: LabelledGeneric.Aux[T, TR],
        columns: ColumnBuilder.Aux[DynamoDBColumn, TR, CR],
        pkCol: Selector.Aux[CR, pk.T, PK],
        skCol: Selector.Aux[CR, sk.T, SK],
        evpk: pk.T <:< Symbol,
        evsk: sk.T <:< Symbol,
        isPK: DynamoDBPKColumn[PK],
        isSK: DynamoDBPKColumn[SK]): DynamoDBTable.Aux[T, CR, PK, SK, HNil] = {
      val cols   = columns()
      val pkName = pk.value.name
      val skName = sk.value.name
      val pkCol  = NamedAttribute.unsafe[PK](cols.columns.find(_._1 == pkName).get)
      val skCol  = cols.columns.find(_._1 == skName).map(NamedAttribute.unsafe[SK])
      DynamoDBTableRepr[T, CR, PK, SK, HNil](name,
                                             pkCol,
                                             skCol,
                                             cols.compose(Iso(lgen.to, lgen.from)),
                                             Seq.empty)
    }
  }

  def flusher: Flushable[S] = new Flushable[S] {
    override def flush =
      writes => {
        val S = effect.S
        val M = S.SM
        S.eval {
          S.drain {
            M.flatMap(S.eval(effect.asyncClient)) { client =>
              S.evalMap(writes) {
                case PutItem(request) =>
                  effect.fromFuture(client.putItem(request))
              }
            }
          }
        }
      }
  }

  def queries: DynamoDBQueries[S, F] = new DynamoDBQueries[S, F](effect)
}
