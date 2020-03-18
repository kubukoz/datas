package tests

import datas.tagless.InvariantTraverseK
import cats.~>
import cats.implicits._
import datas.QueryBase
import datas.schemas.caseClassSchema
import datas.schemas.column
import cats.Show
import doobie.ConnectionIO
import cats.InvariantSemigroupal

object minimal {
  final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

  // Ideally I can derive these instances at compile-time for you
  object User {

    implicit val itraverseK: InvariantTraverseK[User] = new InvariantTraverseK[User] {

      override def itraverseK[F[_], G[_]: InvariantSemigroupal, H[_]](alg: User[F])(fk: F ~> λ[a => G[H[a]]]): G[User[H]] =
        (fk(alg.id), fk(alg.name), fk(alg.age)).imapN(User[H])(u => (u.id, u.name, u.age))
    }

    val schema: QueryBase[User] =
      caseClassSchema(
        "users",
        User(column[Long]("id"), column[String]("name"), column[Int]("age"))
      )

    implicit val showId: Show[User[cats.Id]] = Show.fromToString
  }

  import datas.ops._
  import datas.Reference.lift

  val q: ConnectionIO[List[String]] = User.schema.select(_.name).where(_.age >= lift(18)).compileSql.to[List]
}
