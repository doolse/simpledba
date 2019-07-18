---
layout: home
title: "Home"
section: "home"
---

# simpledba - Simple Database Access for Scala

[![Build Status](https://api.travis-ci.org/doolse/simpledba.svg)](https://travis-ci.org/doolse/simpledba)
[![Coverage Status](https://coveralls.io/repos/github/doolse/simpledba/badge.svg?branch=master)](https://coveralls.io/github/doolse/simpledba?branch=master)

## TL;DR

Map case classes into columns for your DB, create typesafe queries that run with your favourite effect and streaming library.

# Goals

- Prevent you from writing queries which the database won't allow you to run, using types.
- Support as many column family style DB's as possible. (DynamoDB, Cassandra, Google Cloud Datastore)
- Support JDBC with full typed support for non joined queries. Joined queries can be created with un-typechecked SQL.
- Facilitate writing DB agnostic query layers. Switching between AWS and Google Cloud? No Problem.
- Be a library, not a framework. No magic, just principles.

## Quickstart

```scala
val simpledbaVersion = "@VERSION@"

libraryDependencies ++= Seq(
  "io.doolse" %% "simpledba-jdbc",
  "io.doolse" %% "simpledba-dynamodb",
  "io.doolse" %% "simpledba-circe"
).map(_ % simpledbaVersion)
```

Define your domain model case classes and a database agnostic query trait.
In this example we will be using ZIO with it's stream library for the effects.

```scala mdoc
import java.util.UUID

import io.doolse.simpledba._
import zio._
import zio.stream._

case class User(userId: UUID, firstName: String, lastName: String, yearOfBirth: Int)

case class Car(id: UUID, make: String, model: String, ownerId: UUID)

// Support multiple DB types by parameterizing on the write operation type
trait Queries[W] {
  type S[A] = Stream[Throwable, A]
  type F[A] = Task[A]
  type Writes[A] = WriteQueries[S, F, W, A]
  def users: Writes[User]
  def cars: Writes[Car]
  def usersByFirstName: String => S[User]
  def carsForUser: UUID => S[Car]
  def flush: S[W] => F[Unit]
}
```

Write your app:

```scala mdoc

trait ExampleApp[W] {

  type S[A] = Stream[Throwable, A]
  type F[A] = Task[A]

  def queries: Queries[W]
  def initSchema: F[Unit]

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

  import zio.console._

  def querySomeData: TaskR[Console, Unit] =
    (for {
      user <- queries.usersByFirstName("Jolse")
      _ <- ZStream.fromEffect {
        queries.carsForUser(user.userId).runCollect.tap { cars =>
          putStrLn(s"${user.firstName} ${user.lastName} owns ${cars}")
        }
      }
    } yield ()).runDrain

  def run() = {
    val runtime = new DefaultRuntime {}
    runtime.unsafeRun(initSchema *> initData *> querySomeData)
  }
}
```

Now we can create a DB specific implementation of the app, let's start with JDBC (using embedded HSQL).

```scala mdoc
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.interop.zio._
import zio.interop.catz._

class JDBCApp(logger: JDBCLogger[Task]) extends ExampleApp[JDBCWriteOp] {

  import java.sql.DriverManager
  import io.doolse.simpledba.jdbc.hsql._

  // Use HSQL driver, single connection
  val mapper           = hsqldbMapper
  val singleConnection = DriverManager.getConnection("jdbc:hsqldb:mem:example")

  val jdbcQueries = mapper.queries(
    new JDBCEffect[S, F](ZIO.succeed(singleConnection), _ => ZIO.unit, logger))

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
```

Running this gives you the expected result:

```scala
new JDBCApp(NothingLogger()).run()
```

````scala mdoc:passthrough
println("```")
new JDBCApp(NothingLogger()).run()
println("```")
````

To see what's going on with SQL being executed, you can log the queries:

````scala mdoc:passthrough
println("```sql")
new JDBCApp(PrintLnLogger()).run()
println("```")
````
