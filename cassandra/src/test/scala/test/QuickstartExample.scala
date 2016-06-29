package test

/**
  * Created by jolz on 9/06/16.
  */
import java.util.UUID

import io.doolse.simpledba._
import io.doolse.simpledba.cassandra._
import fs2.interop.cats._

object QuickstartExample extends App {
  case class User(userId: UUID, firstName: String, lastName: String, yearOfBirth: Int)

  case class Car(id: UUID, make: String, model: String, ownerId: UUID)

  case class Queries[F[_]](users: WriteQueries[F, User],
                           cars: WriteQueries[F, Car],
                           userByKey: UniqueQuery[F, User, UUID],
                           usersByFirstName: SortableQuery[F, User, String],
                           carsForUser: RangeQuery[F, Car, UUID, String]
                          )

  val model = RelationModel(
    relation[User]('user).key('userId),
    relation[Car]('car).key('id)
  ).queries[Queries](
    writes('user),
    writes('car),
    queryByPK('user),
    query('user).multipleByColumns('firstName),
    query('car).multipleByColumns('ownerId).sortBy('make)
  )

  val mapper = new CassandraMapper()
  val built = mapper.buildModel(model)
  val queries = built.queries
  val sessionConfig = CassandraSession(CassandraIO.simpleSession("localhost"), c => Console.println(c()))
  CassandraUtils.initKeyspaceAndSchema(sessionConfig, "test", built.ddl, dropKeyspace = true).unsafeRun

  private val magId = UUID.randomUUID()
  private val mahId = UUID.randomUUID()
  println {
    (for {
      _ <- queries.users.insert(User(magId, "Jolse", "Maginnis", 1980))
      _ <- queries.users.insert(User(mahId, "Jolse", "Mahinnis", 1999))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Honda", "Accord Euro", magId))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Honda", "Civic", magId))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Ford", "Laser", magId))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Hyundai", "Accent", magId))
      cars <- queries.carsForUser(magId, lower = Exclusive("Honda"))
      users <- queries.usersByFirstName("Jolse")
    } yield (cars ++ users).mkString("\n")).run(sessionConfig).unsafeRun
  }
}