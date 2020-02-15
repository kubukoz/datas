package tests

import doobie.Transactor
import doobie.implicits._
import cats.effect.{test => _, _}
import cats.implicits._
import cats.~>
import fs2.Pipe
import cats.Show
import com.softwaremill.diffx.Diff
import cats.Apply
import datas.tagless.TraverseK
import datas.Query
import datas.Reference
import flawless._
import flawless.data.Assertion

final class BasicJoinQueryTests(implicit xa: Transactor[IO]) extends SuiteClass[IO] {
  import flawless.syntax._

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
    case (a, b, c, d, e, f, g, h, i) =>
      show"($a, $b, $c, $d, $e, $f, $g, $h, $i)"
  }

  import datas.ops._

  def runSuite: Suite[IO] =
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
        val q = User.schema.select(_.name)

        expectAllToBe(q)("Jon", "Jakub", "John")
      },
      test("select second column from table") {
        val q = User.schema.select(_.age)

        expectAllToBe(q)(36, 23, 40)
      },
      test("select + map") {
        val q = User.schema.select(_.name.map(_ + "X"))

        expectAllToBe(q)("JonX", "JakubX", "JohnX")
      },
      test("select lifted constant") {
        val q = User.schema.select(_ => Reference.lift(1))

        expectAllToBe(q)(1, 1, 1)
      },
      test("select lifted + mapped constant") {
        val q = User.schema.select(_ => Reference.lift(1).map(_ + 1))

        expectAllToBe(q)(2, 2, 2)
      },
      test("select option-lifted constant") {
        val q = User.schema.select(_ => Reference.liftOption(Reference.lift(1)))

        expectAllToBe(q)(1.some, 1.some, 1.some)
      },
      test("select equality of same field") {
        val q = User.schema.select(u => equal(u.name, u.name))

        expectAllToBe(q)(true, true, true)
      },
      test("select equality of field with constant") {
        val q = User.schema.select(u => equal(u.name, Reference.lift("Jon")))

        expectAllToBe(q)(true, false, false)
      }
    )

  def singleTableTests =
    tests(
      test("select two columns from single table") {

        val q =
          User
            .schema
            .select(u =>
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
          User
            .schema
            .select(u =>
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
          User
            .schema
            .select(u =>
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
          User.schema.select { u =>
            (
              Reference.lift(true),
              Reference.liftOption(Reference.lift(5L)),
              u.age.as(false)
            ).tupled
          }

        expectAllToBe(q)(
          List(
            (true, Some(5L), false),
            (true, Some(5L), false),
            (true, Some(5L), false)
          ): _*
        )
      },
      test("select all from user") {
        val q =
          User.schema.selectAll.where(u => equal(u.name, Reference.lift("Jon")))

        expectAllToBe(q)(User[cats.Id](1L, "Jon", 36))
      }
    )

  def innerJoinTests = tests(
    test("inner join users and books") {
      val q = User
        .schema
        .innerJoin(Book.schema) { (u, b) =>
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
      val q = User
        .schema
        .innerJoin(Book.schema) { (u, b) =>
          equal(u.id, b.userId)
        }
        .innerJoin(Book.schema) { (t, b) =>
          equal(t.right.parentId, b.id.map(_.some))
        }
        .select {
          _.asTuple.leftMap(_.asTuple) match {
            case ((user, book), bookParent) =>
              (user.name, book.id, bookParent.id).tupled
          }
        }

      expectAllToBe(q)(
        ("John", 2L, 1L)
      )
    },
    test("a join (b join c)") {
      val q = User
        .schema
        .innerJoin(Book.schema.innerJoin(Book.schema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }) { (u, t) =>
          equal(u.id, t.left.userId)
        }
        .select {
          _.asTuple.map(_.asTuple) match {
            case (user, (book, bookParent)) =>
              (user.name, book.id, bookParent.id).tupled
          }
        }

      expectAllToBe(q)(
        ("John", 2L, 1L)
      )
    },
    test("(((a join b) join c) join d)") {
      val q = Book
        .schema
        .innerJoin(Book.schema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }
        .innerJoin(
          User.schema
        ) { (t, u) =>
          equal(u.id, t.left.userId)
        }
        .innerJoin(User.schema) { (t, u) =>
          equal(u.id, t.left.left.userId)
        }
        .select(_.right.id)

      expectAllToBe(q)(
        (3L)
      )
    },
    test("(((a join b) join c) join d) join (((e join f) join g) join h)") {
      val q = Book
        .schema
        .innerJoin(Book.schema) { (b, bP) =>
          equalOptionL(b.parentId, bP.id)
        }
        .innerJoin(
          User.schema
        ) { (t, u) =>
          equal(u.id, t.left.userId)
        }
        .innerJoin(User.schema) { (t, u) =>
          equal(u.id, t.left.left.userId)
        }

      val superQ = q
        .innerJoin(q) { (a, b) =>
          equal(a.left.left.left.userId, b.right.id)
        }
        .select(x =>
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
      val inner = User
        .schema
        .innerJoin(
          Book.schema.innerJoin(Book.schema) { (b, bP) =>
            equalOptionL(b.parentId, bP.id)
          }
        ) { (u, t) =>
          equal(u.id, t.left.userId)
        }

      val q = User
        .schema
        .innerJoin(
          inner
        ) { (u, t) =>
          equal(u.id, t.right.right.userId)
        }

      expectAllToBe(q.select(_.left.id))(
        2L
      )
    },
    test("(a join b) join (a join b)") {
      val inner = Book.schema.innerJoin(Book.schema) { (b, bP) =>
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
      val q = User
        .schema
        .leftJoin(Book.schema) { (u, b) =>
          equal(u.id, b.userId)
        }
        .select { t =>
          val user = t.left
          val book = t.right

          (user.name, book.underlying.name.value).tupled
        }

      expectAllToBe(q)(
        ("Jakub", "Book 1".some),
        ("John", "Book 2".some),
        ("Jon", none)
      )
    }
  )

  val debugOn = false

  def debug[A]: Pipe[IO, A, A] =
    if (debugOn) _.evalTap(s => IO(println(s))) else identity

  def expectAllToBe[A[_[_]], Queried: Show: Diff](
    q: Query[A, Queried]
  )(
    expectedList: Queried*
  )(
    implicit xa: Transactor[IO]
  ): IO[Assertion] = {
    val showQuery =
      if (debugOn) IO(println(show"Testing query: ${q.compileSql.sql}"))
      else IO.unit

    showQuery *> q.compileSql.stream.transact(xa).through(debug).compile.toList.attempt.map {
      case Left(exception) =>
        Assertion.failed(
          show"""An exception occured, but ${expectedList.toList} was expected.
              |Relevant query: ${pprint.apply(q).render}${scala.Console.RED}
              |Compiled: ${q.compileSql.sql}
              |Exception message: ${exception.getMessage}""".stripMargin
        )
      case Right(values) => ensure(values, equalTo(expectedList.toList))
    }
  }
}
import datas.schemas._
import datas.QueryBase

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

object User {

  implicit val traverseK: TraverseK[User] = new TraverseK[User] {

    override def traverseK[F[_], G[_]: Apply, H[_]](alg: User[F])(fk: F ~> λ[a => G[H[a]]]): G[User[H]] =
      (fk(alg.id), fk(alg.name), fk(alg.age)).mapN(User[H])
  }

  val schema: QueryBase[User] =
    caseClassSchema(
      "users",
      User(column[Long]("id"), column[String]("name"), column[Int]("age"))
    )

  implicit val showId: Show[User[cats.Id]] = Show.fromToString
}

final case class Book[F[_]](id: F[Long], userId: F[Long], parentId: F[Option[Long]], name: F[String])

object Book {

  implicit val sequenceK: TraverseK[Book] = new TraverseK[Book] {

    override def traverseK[F[_], G[_]: Apply, H[_]](alg: Book[F])(fk: F ~> λ[a => G[H[a]]]): G[Book[H]] =
      (fk(alg.id), fk(alg.userId), fk(alg.parentId), fk(alg.name)).mapN(Book[H])
  }

  val schema: QueryBase[Book] =
    caseClassSchema(
      "books",
      Book(
        column[Long]("id"),
        column[Long]("user_id"),
        column[Long]("parent_id").optional,
        column[String]("name")
      )
    )
}
