package io.doolse.example

import java.sql.DriverManager
import java.util.UUID

import io.doolse.simpledba.WriteQueries
import io.doolse.simpledba.jdbc.{JDBCEffect, JDBCWriteOp}
import zio.stream.Stream
import zio._

object Example extends scala.App {

  case class User(userId: UUID, firstName: String, lastName: String, yearOfBirth: Int)

  case class Car(id: UUID, make: String, model: String, ownerId: UUID)

  trait Queries[W] {
    type S[A]      = Stream[Throwable, A]
    type F[A]      = Task[A]
    type Writes[A] = WriteQueries[S, F, W, A]

    def users: Writes[User]

    def cars: Writes[Car]

    def usersByFirstName: String => S[User]

    def carsForUser: UUID => S[Car]

    def flush: S[W] => F[Unit]
  }

  import zio.console._

  trait ExampleApp[W] {

    type S[A] = Stream[Throwable, A]
    type F[A] = Task[A]

    val queries: Queries[W]
    val initSchema: F[Unit]

    def initData: F[Unit] = {
      val magId = UUID.randomUUID()
      val mahId = UUID.randomUUID()
      queries.flush {
        queries.users.insertAll(
          Stream(User(magId, "Jolse", "Maginnis", 1980), User(mahId, "Jolse", "Mahinnis", 1999))) ++
          queries.cars.insertAll(
            Stream(
              Car(UUID.randomUUID(), "Honda", "Accord Euro", magId),
              Car(UUID.randomUUID(), "Honda", "Civic", mahId),
              Car(UUID.randomUUID(), "Ford", "Laser", magId),
              Car(UUID.randomUUID(), "Hyundai", "Accent", magId)
            ))
      }
    }

    def querySomeData: TaskR[Console, Unit] =
      (for {
        user <- queries.usersByFirstName("Jolse")
        _ <- Stream.fromEffect {
          queries.carsForUser(user.userId).runCollect.tap { cars =>
            putStrLn(s"${user.firstName} ${user.lastName} owns ${cars}")
          }
        }
      } yield ()).runDrain

    val runtime = new DefaultRuntime {}

    runtime.unsafeRun(initSchema *> initData *> querySomeData)
  }

  new ExampleApp[JDBCWriteOp] {

    import io.doolse.simpledba.jdbc.hsql._
    import io.doolse.simpledba.jdbc._
    import io.doolse.simpledba.interop.zio._
    import zio.interop.catz._

    val mapper           = hsqldbMapper
    val singleConnection = DriverManager.getConnection("jdbc:hsqldb:mem:example")
    val jdbcQueries = mapper.queries(
      new JDBCEffect[S, F](SingleJDBCConnection(singleConnection), PrintLnLogger()))
    val carTable  = mapper.mapped[Car].table("cars").key('id)
    val userTable = mapper.mapped[User].table("users").key('userId)

    import jdbcQueries._

    override val queries: Queries[JDBCWriteOp] = new Queries[JDBCWriteOp] {
      val users: Writes[User]                 = writes(userTable)
      val cars: Writes[Car]                   = writes(carTable)
      val usersByFirstName: String => S[User] = query(userTable).where('firstName, BinOp.EQ).build
      val carsForUser: UUID => S[Car]         = query(carTable).where('ownerId, BinOp.EQ).build
      val flush: S[JDBCWriteOp] => F[Unit]    = jdbcQueries.flush
    }
    override val initSchema: F[Unit] = jdbcQueries.flush {
      Stream(carTable, userTable).flatMap(dropAndCreate)
    }
  }

  //
  //    println {
  //      (for {
  //        _ <- queries.users.insert()
  //        _ <- queries.users.insert()
  //        _ <- queries.cars.insert(Car(UUID.randomUUID(), "Honda", "Accord Euro", magId))
  //        _ <- queries.cars.insert(Car(UUID.randomUUID(), "Honda", "Civic", magId))
  //        _ <- queries.cars.insert(Car(UUID.randomUUID(), "Ford", "Laser", magId))
  //        _ <- queries.cars.insert(Car(UUID.randomUUID(), "Hyundai", "Accent", magId))
  //        cars <- queries.carsForUser(magId, lower = Exclusive("Honda")).runLog
  //        users <- queries.usersByFirstName("Jolse").runLog
  //      } yield (cars ++ users).mkString("\n")).run(sessionConfig).unsafeRunSync()
  //    }
  //  }

//
//    val mapper = hsqldbMapper
//    val singleConnection = connectionFromConfig()
//    val jdbcQueries = mapper.queries(new JDBCEffect[S](ZIO.succeed(singleConnection), _ => ZIO.unit))
//    val carTable = mapper.mapped[Car].table("cars") .key('id)
//    val jdbcQueries = new Queries[JDBCWriteOp] {
//      override def users: Writes[User] = ???
//
//      override def cars: Writes[Car] = ???
//
//      override def usersByFirstName: String => S[User] = ???
//
//      override def carsForUser: UUID => S[Car] = ???
//
//      override def flush: S[JDBCWriteOp] => F[Unit] = ???
//    }
//  }
}
