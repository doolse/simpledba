package io.doolse.simpledba.test.jdbc

import zio.Task
import zio.stream.ZStream

object JDBCExpressionProperties extends JDBCProperties[ZStream[Any, Throwable, ?], Task] with ZIOProperties {

}
