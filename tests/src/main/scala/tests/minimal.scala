package tests

import cats.Apply
import cats.Show
import cats.implicits._
import cats.~>
import datas.QueryBase
import datas.schemas.caseClassSchema
import datas.schemas.column
import datas.tagless.TraverseK
import doobie.ConnectionIO

object minimal {
  final case class User[F[_]](id: F[Long], name: F[String], age: F[Int], middleName: F[Option[String]])

  // Ideally I can derive these instances at compile-time for you
  object User {

    implicit val traverseK: TraverseK[User] = new TraverseK[User] {

      override def traverseK[F[_], G[_]: Apply, H[_]](alg: User[F])(fk: F ~> λ[a => G[H[a]]]): G[User[H]] =
        (
          fk(alg.id),
          fk(alg.name),
          fk(alg.age),
          fk(alg.middleName),
        ).mapN(User[H])

    }

    val schema: QueryBase[User] =
      caseClassSchema(
        "users",
        User(
          column[Long]("id"),
          column[String]("name"),
          column[Int]("age"),
          column[String]("middle_name").optional,
        ),
      )

    implicit val showId: Show[User[cats.Id]] = Show.fromToString
  }

  import datas.ops._
  import datas.Reference.lift

  val q: ConnectionIO[List[String]] = User.schema.select(_.name).where(_.age >= lift(18)).compileSql.to[List]

  val qWhole: ConnectionIO[List[User[cats.Id]]] = User.schema.selectAll.where(_.age >= lift(18)).compileSql.to[List]
}
