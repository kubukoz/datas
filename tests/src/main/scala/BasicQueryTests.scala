import doobie.Transactor
import doobie.implicits._
import flawless._
import cats.effect._
import cats.implicits._
import cats.tagless.FunctorK
import cats.~>
import fs2.Pipe
import cats.kernel.Eq
import cats.Show

object BasicJoinQueryTests {

  implicit def showTuple3[A: Show, B: Show, C: Show]: Show[(A, B, C)] = {
    case (a, b, c) => show"($a, $b, $c)"
  }

  import flawless.syntax._
  import datas._

  def runSuite(implicit xa: Transactor[IO]): Tests[SuiteResult] =
    singleTableTests |+| singleJoinTests

  def singleTableTests(implicit xa: Transactor[IO]) = tests(
    test("basic query from single table") {

      val q =
        userSchema.select(
          u =>
            (
              u.name,
              u.age
            ).tupled
        )

      expectAllToBe(q)(
        ("Jon", 36),
        ("Jakub", 23),
        ("John", 40)
      )
    },
    test("querying equalities") {

      val q =
        userSchema.select(
          u =>
            (
              equal(u.age, Reference.lift(23)),
              equal(Reference.lift(5), Reference.lift(10))
            ).tupled
        )

      expectAllToBe(q)(
        (false, false),
        (true, false),
        (false, false)
      )
    },
    test("conditions in queries") {

      val q =
        userSchema
          .select(
            u =>
              (
                u.name,
                u.age
              ).tupled
          )
          .where(u => over(u.age, Reference.lift(24)))
          .where(u => nonEqual(u.name, Reference.lift("John")))

      expectAllToBe(q)(
        ("Jon", 36)
      )
    },
    test("querying custom references") {

      val q =
        userSchema.select(
          u =>
            (
              Reference.lift(true),
              Reference.lift(Option(5L)),
              u.age.as(false)
            ).tupled
        )

      expectAllToBe(q)(
        (true, Some(5L), false),
        (true, Some(5L), false),
        (true, Some(5L), false)
      )
    }
  )

  def singleJoinTests(implicit xa: Transactor[IO]) = tests(
    test("join users and books") {
      val q = userSchema
        .leftJoin(bookSchema) { (u, b) =>
          equal(u.id, b.userId)
        }
        .select {
          _.asTuple match {
            case (user, book) => (user.name, book.id).tupled
          }
        }

      expectAllToBe(q)(
        ("Jon", 0),
        ("Jakub", 1L),
        ("John", 2L)
      )
    }
  )

  def debug[A]: Pipe[IO, A, A] = if (false) _.evalTap(s => IO(println(s))) else identity

  def expectAllToBe[A[_[_]], Queried: Eq: Show](
    q: Query[A, Queried]
  )(
    first: Queried,
    rest: Queried*
  )(
    implicit xa: Transactor[IO]
  ): IO[Assertions] =
    q.compileSql.stream.transact(xa).through(debug).compile.toList.attempt.map {
      _.leftMap(_.getMessage) shouldBe Right(first :: rest.toList)
    }

  import datas.schemas._

  val userSchema: TableQuery[User] =
    caseClassSchema(
      TableName("users"),
      (column[Long]("id"), column[String]("name"), column[Int]("age")).mapN(User[Reference])
    )

  val bookSchema: TableQuery[Book] =
    caseClassSchema(
      TableName("books"),
      (column[Long]("id"), column[Long]("user_id"), column[Option[Long]]("parent_id")).mapN(Book[Reference])
    )

}

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

object User {
  implicit val functorK: FunctorK[User] = new FunctorK[User] {
    def mapK[F[_], G[_]](af: User[F])(fk: F ~> G): User[G] = User(fk(af.id), fk(af.name), fk(af.age))
  }
}

final case class Book[F[_]](id: F[Long], userId: F[Long], parentId: F[Option[Long]])

object Book {
  implicit val functorK: FunctorK[Book] = new FunctorK[Book] {
    def mapK[F[_], G[_]](af: Book[F])(fk: F ~> G): Book[G] = Book(fk(af.id), fk(af.userId), fk(af.parentId))
  }
}
