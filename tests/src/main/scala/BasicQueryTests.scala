import doobie.Transactor
import doobie.implicits._
import flawless.dsl._
import flawless.predicates.all._
import cats.effect.{test => _, _}
import cats.implicits._
import cats.tagless.FunctorK
import cats.~>
import fs2.Pipe
import cats.Show
import cats.data.NonEmptyList
import com.softwaremill.diffx.Diff
import flawless.data.Suite
import flawless.data.Assertion

final class BasicJoinQueryTests(implicit xa: Transactor[IO]) {

  implicit def showTuple3[A: Show, B: Show, C: Show]: Show[(A, B, C)] = {
    case (a, b, c) => show"($a, $b, $c)"
  }

  implicit def showTuple9[
    A1: Show,
    A2: Show,
    A3: Show,
    A4: Show,
    A5: Show,
    A6: Show,
    A7: Show,
    A8: Show,
    A9: Show
  ]: Show[(A1, A2, A3, A4, A5, A6, A7, A8, A9)] = {
    case (a, b, c, d, e, f, g, h, i) => show"($a, $b, $c, $d, $e, $f, $g, $h, $i)"
  }

  import datas._

  def run: Suite[IO] =
    suite("BasicQueryTests") {
      (
        singleColumnTests
          |+| singleTableTests
          |+| innerJoinTests
          |+| leftJoinTests
      )
    }

  def singleColumnTests =
    tests(
      test("select single column from table") {
        val q = userSchema.select(_.name)

        expectAllToBe(q)("Jon", "Jakub", "John")
      },
      test("select second column from table") {
        val q = userSchema.select(_.age)

        expectAllToBe(q)(36, 23, 40)
      },
      test("select + map") {
        val q = userSchema.select(_.name.map(_ + "X"))

        expectAllToBe(q)("JonX", "JakubX", "JohnX")
      },
      test("select lifted constant") {
        val q = userSchema.select(_ => Reference.lift(1))

        expectAllToBe(q)(1, 1, 1)
      },
      test("select lifted + mapped constant") {
        val q = userSchema.select(_ => Reference.lift(1).map(_ + 1))

        expectAllToBe(q)(2, 2, 2)
      },
      test("select option-lifted constant") {
        val q = userSchema.select(_ => Reference.liftOption(Reference.lift(1)))

        expectAllToBe(q)(1.some, 1.some, 1.some)
      },
      test("select equality of same field") {
        val q = userSchema.select(u => equal(u.name, u.name))

        expectAllToBe(q)(true, true, true)
      },
      test("select equality of field with constant") {
        val q = userSchema.select(u => equal(u.name, Reference.lift("Jon")))

        expectAllToBe(q)(true, false, false)
      }
    )

  def singleTableTests =
    tests(
      test("select two columns from single table") {

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
          userSchema.select { u =>
            (
              Reference.lift(true),
              Reference.liftOption(Reference.lift(5L)),
              u.age.as(false)
            ).tupled
          }

        expectAllToBe(q)(
          List((true, Some(5L), false), (true, Some(5L), false), (true, Some(5L), false)): _*
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
    test("(((a join b) join c) join d)") {
      val q = bookSchema
        .innerJoin(bookSchema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }
        .innerJoin(
          userSchema
        ) { (t, u) =>
          equal(u.id, t.left.userId)
        }
        .innerJoin(userSchema) { (t, u) =>
          equal(u.id, t.left.left.userId)
        }
        .select(_.right.id)

      expectAllToBe(q)(
        (3L)
      )
    },
    test("(((a join b) join c) join d) join (((e join f) join g) join h)") {
      val q = bookSchema
        .innerJoin(bookSchema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }
        .innerJoin(
          userSchema
        ) { (t, u) =>
          equal(u.id, t.left.userId)
        }
        .innerJoin(userSchema) { (t, u) =>
          equal(u.id, t.left.left.userId)
        }

      val superQ = q
        .innerJoin(q) { (a, b) =>
          equal(a.left.left.left.userId, b.right.id)
        }
        .select(
          x =>
            (
              x.left.left.left.left.id,
              x.left.left.left.right.id,
              x.left.left.right.id,
              x.left.right.id,
              x.right.left.left.left.id,
              x.right.left.left.right.id,
              x.right.left.left.right.parentId,
              x.right.left.right.id,
              x.right.right.id
            ).tupled
        )

      expectAllToBe(superQ)(
        (2L, 1L, 3L, 3L, 2L, 1L, None, 3L, 3L)
      )
    },
    test("a join (b join (c join d))") {
      val inner = userSchema.innerJoin(
        bookSchema.innerJoin(bookSchema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }
      ) { (u, t) =>
        equal(u.id, t.left.userId)
      }

      val q = userSchema.innerJoin(
        inner
      ) { (u, t) =>
        equal(u.id, t.right.right.userId)
      }

      expectAllToBe(q.select(_.left.id))(
        2L
      )
    },
    test("(a join b) join (a join b)") {
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
        (2L)
      )
    }
  )

  def leftJoinTests = tests(
    test("left join users and books") {
      val q = userSchema
        .leftJoin(bookSchema) { (u, b) =>
          equal(u.id, b.userId)
        }
        .select {
          _.asTuple match {
            case (user, book) =>
              (user.name, book.underlying.name.value).tupled
          }
        }

      expectAllToBe(q)(
        ("Jakub", "Book 1".some),
        ("John", "Book 2".some),
        ("Jon", none)
      )
    }
  )

  val debugOn = false
  def debug[A]: Pipe[IO, A, A] = if (debugOn) _.evalTap(s => IO(println(s))) else identity

  def expectAllToBe[A[_[_]], Queried: Show: Diff](
    q: Query[A, Queried]
  )(
    expectedList: Queried*
  )(
    implicit xa: Transactor[IO]
  ): IO[NonEmptyList[Assertion]] = {
    val showQuery = (if (debugOn) IO(println(show"Testing query: ${q.compileSql.sql}")) else IO.unit)

    showQuery *> q.compileSql.stream.transact(xa).through(debug).compile.toList.attempt.map {
      case Left(exception) =>
        Assertion
          .Failed(
            show"""An exception occured, but ${expectedList.toList} was expected.
              |Relevant query: ${pprint.apply(q).render}${scala.Console.RED}
              |Compiled: ${q.compileSql.sql}
              |Exception message: ${exception.getMessage}""".stripMargin
          )
          .pure[NonEmptyList]
      case Right(values) => ensure(values, equalTo(expectedList.toList))
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
      (column[Long]("id"), column[Long]("user_id"), column[Long]("parent_id").map(Reference.liftOption(_)), column[String]("name"))
        .mapN(Book[Reference])
    )

}

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

object User {
  implicit val functorK: FunctorK[User] = new FunctorK[User] {
    def mapK[F[_], G[_]](af: User[F])(fk: F ~> G): User[G] = User(fk(af.id), fk(af.name), fk(af.age))
  }
}

final case class Book[F[_]](id: F[Long], userId: F[Long], parentId: F[Option[Long]], name: F[String])

object Book {
  implicit val functorK: FunctorK[Book] = new FunctorK[Book] {

    def mapK[F[_], G[_]](af: Book[F])(fk: F ~> G): Book[G] =
      Book(id = fk(af.id), userId = fk(af.userId), parentId = fk(af.parentId), name = fk(af.name))
  }
}
