package test

/**
  * Created by jolz on 9/06/16.
  */
import java.util.UUID

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import fs2.interop.cats._
import io.doolse.simpledba._
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}

object QuickstartExampleDynamo extends App {
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

  val mapper = new DynamoDBMapper()
  val built = mapper.buildModel(model)
  val queries = built.queries

  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client: AmazonDynamoDBClient = new AmazonDynamoDBClient(config).withEndpoint("http://localhost:8000")

  DynamoDBUtils.createSchema(client, built.ddl)
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
      cars <- queries.carsForUser(magId, lower = "Ford", higher = Exclusive("Hyundai"))
      users <- queries.usersByFirstName("Jolse")
    } yield (cars ++ users).mkString("\n")).run(DynamoDBSession(client))
  }
}