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
import flawless.stats.Location

final class BasicJoinQueryTests(implicit xa: Transactor[IO]) {

  implicit def showTuple3[A: Show, B: Show, C: Show]: Show[(A, B, C)] = {
    case (a, b, c) => show"($a, $b, $c)"
  }

  import flawless.syntax._
  import datas._

  def run: Tests[SuiteResult] =
    singleTableTests |+| innerJoinTests

  def singleTableTests = tests(
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

  def innerJoinTests = tests(
    test("inner join users and books") {
      val q = userSchema
        .innerJoin(bookSchema) { (u, b) =>
          equal(u.id, b.userId)
        }
        .select {
          _.asTuple match {
            case (user, book) => (user.name, book.id).tupled
          }
        }

      expectAllToBe(q)(
        ("Jakub", 1L),
        ("John", 2L)
      )
    },
    test("(a join b) join c)") {
      val q = userSchema
        .innerJoin(bookSchema) { (u, b) =>
          equal(u.id, b.userId)
        }
        .innerJoin(bookSchema) { (t, b) =>
          equal(t.right.parentId, b.id.map(_.some))
        }
        .select {
          _.asTuple.leftMap(_.asTuple) match {
            case ((user, book), bookParent) => (user.name, book.id, bookParent.id).tupled
          }
        }

      expectAllToBe(q)(
        ("John", 2L, 1L)
      )
    },
    test("a join (b join c)") {
      val q = userSchema
        .innerJoin(bookSchema.innerJoin(bookSchema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }) { (u, t) =>
          equal(u.id, t.left.userId)
        }
        .select {
          _.asTuple.map(_.asTuple) match {
            case (user, (book, bookParent)) => (user.name, book.id, bookParent.id).tupled
          }
        }

      expectAllToBe(q)(
        ("John", 2L, 1L)
      )
    },
    test("(a join b) join (a join b)") {
      //todo this needs some more thinking - maybe when compiling "from" don't return lists but trees? identifiers may come in different places than "on" clauses
      val inner = bookSchema.innerJoin(bookSchema) { (b, bP) =>
        equalOptionL(b.parentId, bP.id)
      }

      val q = inner
        .innerJoin(inner) { (a, b) =>
          equalOptionL(a.left.parentId, b.right.id)
        }
        .select { a =>
          (a.left.left.id)
        }

      expectAllToBe(q)(
        (1L)
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
    implicit xa: Transactor[IO],
    file: sourcecode.File,
    line: sourcecode.Line
  ): IO[Assertions] = {
    val expectedList = first :: rest.toList

    IO(println(show"Testing query: ${q.compileSql.sql}")) *> q.compileSql.stream.transact(xa).through(debug).compile.toList.attempt.map {
      case Left(exception) =>
        Assertions(
          Assertion.Failed(
            AssertionFailure(
              show"""An exception occured, but $expectedList was expected.
              |Relevant query: ${pprint.apply(q).render}${scala.Console.RED}
              |Compiled: ${q.compileSql.sql}
              |Exception message: ${exception.getMessage}""".stripMargin,
              Location(file.value, line.value)
            )
          )
        )
      case Right(values) => values shouldBe values
    }
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
