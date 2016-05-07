package test

import cats.Monad
import cats.syntax.all._
import io.doolse.simpledba.{ColumnName, RelationIO, SelectQuery}

/**
  * Created by jolz on 5/05/16.
  */
object TestQuery {

  def doQuery[F[_] : Monad, RSOps[_]: Monad](db: RelationIO[F, RSOps])(boolCol: db.CT[Boolean], stringCol: db.CT[String], longCol: db.CT[Long]) = {
    import db._
    val rsOps = for {
      _ <- resultSetOperations.nextResult
      enabled <- resultSetOperations.getColumn(ColumnName("enabled"), boolCol)
      pwd <- resultSetOperations.getColumn(ColumnName("adminpassword"), stringCol)
      unknown <- resultSetOperations.getColumn(ColumnName("timezone"), stringCol)
    } yield (enabled, unknown, pwd)

    for {
      rs <- query(SelectQuery("institution",  List(ColumnName("timezone"), ColumnName("enabled"), ColumnName("adminpassword")), List(ColumnName("uniqueid")), None),
        Iterable(parameter(longCol, 517573426L)).asInstanceOf[Iterable[QP[Any]]])
      r <- usingResults(rs, rsOps)
    } yield r
  }
}
