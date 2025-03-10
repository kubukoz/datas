package tests

import doobie.Transactor
import doobie.implicits._
import cats.effect._
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
import flawless.data.Suite
import datas.tagless.Tuple2KK
import datas.tagless.OptionTK
import cats.data.OptionT
import cats.data.NonEmptyList

final class BasicJoinQueryTests(implicit xa: Transactor[IO]) extends SuiteClass[IO] {
  import flawless.syntax._

  implicit def showTuple3[A: Show, B: Show, C: Show]: Show[(A, B, C)] = { case (a, b, c) =>
    show"($a, $b, $c)"
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
    A9: Show,
  ]: Show[(A1, A2, A3, A4, A5, A6, A7, A8, A9)] = { case (a, b, c, d, e, f, g, h, i) =>
    NonEmptyList.of[cats.Show.Shown](a, b, c, d, e, f, g, h, i).map(_.toString).mkString_("(", ", ", ")")
  }

  import datas.ops._

  def runSuite: Suite[IO] =
    suite("BasicQueryTests") {
      NonEmptyList
        .of(
          singleColumnTests,
          singleTableTests,
          innerJoinTests,
          leftJoinTests,
        )
        .reduce
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
      },
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
                u.age,
              ).tupled
            )

        expectAllToBe(q)(
          ("Jon", 36),
          ("Jakub", 23),
          ("John", 40),
        )
      },
      test("querying equalities") {

        val q =
          User
            .schema
            .select(u =>
              (
                equal(u.age, Reference.lift(23)),
                equal(Reference.lift(5), Reference.lift(10)),
              ).tupled
            )

        expectAllToBe(q)(
          (false, false),
          (true, false),
          (false, false),
        )
      },
      test("conditions in queries") {

        val q =
          User
            .schema
            .select(u =>
              (
                u.name,
                u.age,
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
              u.age.as(false),
            ).tupled
          }

        expectAllToBe(q)(
          List(
            (true, Some(5L), false),
            (true, Some(5L), false),
            (true, Some(5L), false),
          ): _*
        )
      },
      test("select all from user") {
        val q =
          User.schema.selectAll.where(u => equal(u.name, Reference.lift("Jon")))

        expectAllToBe(q)(User[cats.Id](1L, "Jon", 36))
      },
      test("select all from simple join") {
        val q = User.schema.innerJoin(Book.schema)(_.id === _.userId).selectAll

        implicit def showAny[A]: Show[A] = Show.fromToString

        expectAllToBe(q)(
          Tuple2KK(User[cats.Id](2, "Jakub", 23), Book[cats.Id](1, 2, None, "Book 1")),
          Tuple2KK(User[cats.Id](3, "John", 40), Book[cats.Id](2, 3, Some(1), "Book 2")),
        )
      },
      test("select all from left join") {

        val q = User.schema.leftJoin(Book.schema)((u, b) => equal(u.id, b.userId)).selectAll

        implicit def showAny[A]: Show[A] = Show.fromToString

        expectAllToBe(q)(
          Tuple2KK(
            User[cats.Id](2, "Jakub", 23),
            OptionTK(
              Book(
                OptionT[cats.Id, Long](1L.some),
                OptionT[cats.Id, Long](2L.some),
                OptionT[cats.Id, Option[Long]](none),
                OptionT[cats.Id, String]("Book 1".some),
              )
            ),
          ),
          Tuple2KK(
            User[cats.Id](3, "John", 40),
            OptionTK(
              Book(
                OptionT[cats.Id, Long](2L.some),
                OptionT[cats.Id, Long](3L.some),
                OptionT[cats.Id, Option[Long]](1L.some.some),
                OptionT[cats.Id, String]("Book 2".some),
              )
            ),
          ),
          Tuple2KK(
            User[cats.Id](1, "Jon", 36),
            OptionTK(
              Book(
                OptionT[cats.Id, Long](none),
                OptionT[cats.Id, Long](none),
                OptionT[cats.Id, Option[Long]](none),
                OptionT[cats.Id, String](none),
              )
            ),
          ),
        )
      },
    )

  def innerJoinTests = tests(
    test("inner join users and books") {
      val q = User.schema.innerJoin(Book.schema)((u, b) => equal(u.id, b.userId)).select {
        _.asTuple match {
          case (user, book) => (user.name, book.id).tupled
        }
      }

      expectAllToBe(q)(
        ("Jakub", 1L),
        ("John", 2L),
      )
    },
    test("(a join b) join c)") {
      val q = User
        .schema
        .innerJoin(Book.schema)((u, b) => equal(u.id, b.userId))
        .innerJoin(Book.schema)((t, b) => equal(t.right.parentId, b.id.map(_.some)))
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
        .innerJoin(Book.schema.innerJoin(Book.schema)((b, bP) => equalOptionL(b.parentId, bP.id)))((u, t) => equal(u.id, t.left.userId))
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
        .innerJoin(Book.schema)((b, bP) => equalOptionL(b.parentId, bP.id))
        .innerJoin(
          User.schema
        )((t, u) => equal(u.id, t.left.userId))
        .innerJoin(User.schema)((t, u) => equal(u.id, t.left.left.userId))
        .select(_.right.id)

      expectAllToBe(q)(
        3L
      )
    },
    test("(((a join b) join c) join d) join (((e join f) join g) join h)") {
      val q = Book
        .schema
        .innerJoin(Book.schema)((b, bP) => equalOptionL(b.parentId, bP.id))
        .innerJoin(
          User.schema
        )((t, u) => equal(u.id, t.left.userId))
        .innerJoin(User.schema)((t, u) => equal(u.id, t.left.left.userId))

      val superQ = q
        .innerJoin(q)((a, b) => equal(a.left.left.left.userId, b.right.id))
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
            x.right.right.id,
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
          Book.schema.innerJoin(Book.schema)((b, bP) => equalOptionL(b.parentId, bP.id))
        )((u, t) => equal(u.id, t.left.userId))

      val q = User
        .schema
        .innerJoin(
          inner
        )((u, t) => equal(u.id, t.right.right.userId))

      expectAllToBe(q.select(_.left.id))(
        2L
      )
    },
    test("(a join b) join (a join b)") {
      val inner = Book.schema.innerJoin(Book.schema)((b, bP) => equalOptionL(b.parentId, bP.id))

      val q = inner.innerJoin(inner)((a, b) => equalOptionL(a.left.parentId, b.right.id)).select(a => (a.left.left.id))

      expectAllToBe(q)(
        2L
      )
    },
  )

  def leftJoinTests =
    tests(
      test("left join users and books") {
        val q = User.schema.leftJoin(Book.schema)((u, b) => equal(u.id, b.userId)).select { t =>
          val user = t.left
          val book = t.right

          (user.name, book.underlying.name.value, book.underlying.parentId.value).tupled
        }

        expectAllToBe(q)(
          ("Jakub", "Book 1".some, none),
          ("John", "Book 2".some, 1L.some.some),
          ("Jon", none, none),
        )
      },
      test("#53 - joining on an optional field results in Some(None)") {
        val q = User.schema.leftJoin(Book.schema)((u, b) => equal(u.id, b.userId)).select(a => a.right.underlying.parentId.value)

        expectAllToBe(q)(none, 1L.some.some, none)
      },
    )

  var debugOn = false

  def debug[A]: Pipe[IO, A, A] =
    if (debugOn) _.debug() else identity

  def expectAllToBe[A[_[_]], Queried: Show: Diff](q: Query[A, Queried])(expectedList: Queried*): IO[Assertion] =
    ensure(q, predicates.resultsAre[A, Queried](equalTo(expectedList.toList)))

  //todo: some of these should go to flawless
  object predicates {

    case class PredicateTrans[F[_], G[_], Inner, Outer](p: PredicateT[F, Inner] => PredicateT[G, Outer]) {

      def compose[H[_], Inner0](another: PredicateT[H, Inner0] => PredicateT[F, Inner]): PredicateTrans[H, G, Inner0, Outer] =
        PredicateTrans(p compose another)
    }

    def resultsAre[A[_[_]], Queried](resultPredicate: Predicate[List[Queried]]): PredicateT[IO, Query[A, Queried]] =
      predicateFlatten { q =>
        streamedQuery(
          listedStream(
            either(
              dumpQueryOnFailure(q),
              resultPredicate,
            ).liftM[IO].contramap(_.attempt)
          )
        )
      }

    def dumpQueryOnFailure[A[_[_]], Queried](q: Query[A, Queried]): Predicate[Throwable] = Predicate { throwable =>
      Assertion.failed(
        show"""An exception occured.
            |Relevant query: ${pprint.apply(q).plainText}
            |Compiled: ${q.compileSql.sql}""".stripMargin
      ) |+| Assertion.thrown(throwable)
    }

    def predicateFlatten[A](f: A => PredicateT[IO, A]): PredicateT[IO, A] = PredicateT(a => f(a).apply(a))

    def streamedQuery[A[_[_]], Queried](streamPredicate: PredicateT[IO, fs2.Stream[IO, Queried]]): PredicateT[IO, Query[A, Queried]] =
      streamPredicate.contramap(_.compileSql.stream.transact(xa).through(debug))

    def listedStream[A](listPredicate: PredicateT[IO, IO[List[A]]]): PredicateT[IO, fs2.Stream[IO, A]] =
      listPredicate.contramap(_.compile.toList)

    def either[X, Y](left: Predicate[X], right: Predicate[Y]): Predicate[Either[X, Y]] = Predicate(_.fold(left.apply, right.apply))
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
      User(column[Long]("id"), column[String]("name"), column[Int]("age")),
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
        column[String]("name"),
      ),
    )

}
