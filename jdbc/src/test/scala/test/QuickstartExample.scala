package test

/**
  * Created by jolz on 9/06/16.
  */
import java.util.UUID

import io.doolse.simpledba._
import io.doolse.simpledba.jdbc._

object QuickstartExample extends App {
  case class User(userId: UUID, firstName: String, lastName: String, yearOfBirth: Int)

  case class Car(id: UUID, make: String, model: String, ownerId: UUID)

  case class Queries[F[_]](users: WriteQueries[F, User],
                           cars: WriteQueries[F, Car],
                           userByKey: UniqueQuery[F, User, UUID],
                           usersByFirstName: SortableQuery[F, User, String],
                           carsForUser: RangeQuery[F, Car, UUID, String]
                          )

  val userRel = relation[User]('user).key('userId)
  val carRel = relation[Car]('car).key('id)
  val model = RelationModel(userRel, carRel
  ).queries[Queries](
    writes(userRel),
    writes(carRel),
    queryByPK(userRel),
    query(userRel).multipleByColumns('firstName),
    query(carRel).multipleByColumns('ownerId).sortBy('make)
  )

  val mapper = new JDBCMapper()
  val built = mapper.buildModel(model)
  val queries = built.queries
  val sessionConfig = JDBCIO.openConnection().copy(logger = msg => Console.err.println(msg()))
  JDBCUtils.createSchema(sessionConfig, built.ddl, drop=true).unsafeRunSync()

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
      cars <- queries.carsForUser(magId, lower = Inclusive("Honda")).compile.toVector
      users <- queries.usersByFirstName("Jolse").compile.toVector
    } yield (cars ++ users).mkString("\n")).runA(sessionConfig).unsafeRunSync()
  }
}