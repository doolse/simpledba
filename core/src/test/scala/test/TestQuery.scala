package test

import cats.Monad
import cats.syntax.all._
import io.doolse.simpledba._
import shapeless.HList

/**
  * Created by jolz on 5/05/16.
  */
object TestQuery {

  def insertAndQuery[F[_]:Monad, T, Key](queries: RelationQueries[F, T, Key, _], insert: T, key: Key): F[Option[T]] = for {
    _ <- queries.insert(insert)
    res <- queries.queryByKey(key)
  } yield res


  def doQuery[F[_] : Monad, RSOps[_]: Monad](db: RelationIO[F, RSOps])(boolCol: db.CT[Boolean], stringCol: db.CT[String], longCol: db.CT[Long]) = {
    import db._
    val rsQ = for {
      _ <- rsOps.nextResult
      enabled <- rsOps.getColumn(ColumnName("enabled"), boolCol)
      pwd <- rsOps.getColumn(ColumnName("adminpassword"), stringCol)
      unknown <- rsOps.getColumn(ColumnName("timezone"), stringCol)
    } yield (enabled, unknown, pwd)

    for {
      rs <- query(SelectQuery("institution",  List(ColumnName("timezone"), ColumnName("enabled"), ColumnName("adminpassword")), List(ColumnName("uniqueid")), None),
        Iterable(parameter(longCol, 517573426L)))
      r <- usingResults(rs, rsQ)
    } yield r
  }
}
