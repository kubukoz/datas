import cats.effect._
import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.data.State
import cats.Applicative
import cats.tagless.FunctorK
import cats.~>
import cats.tagless.implicits._
import cats.Apply
import shapeless.HNil
import shapeless.{:: => HCons}

object datas {
  // def schema[A]: Schema[A] = ???

  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type: Read](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Single(ReferenceData.Column(col, None), Read[Type]))
    }

    def caseClassSchema[F[_[_]]: FunctorK](name: TableName, stClass: ST[F[Reference]]): TableQuery[F] =
      stClass
        .run(Chain.empty)
        .map {
          case (_, data) =>
            val (newGetSymbol, newSymbol) = GetSymbol.initial.next
            TableQuery(QueryBase(name, newSymbol, Nil), data, newGetSymbol)
        }
        .value
  }
  //todo derivation
  //todo make sure schema must have a table

  final case class TableName(name: String) extends AnyVal {
    def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  final case class QueryBase(table: TableName, tableSymbol: String, joins: List[Join]) {

    private def compileJoinAsFrom(join: Join): Fragment =
      Fragment.const(join.kind) ++
        join.withTable.identifierFragment ++
        Fragment.const(" " + join.withTableSymbol) ++
        fr"on" ++
        join.onClause.compile.frag

    def compileAsFrom: Fragment =
      table.identifierFragment ++ Fragment.const(" " + tableSymbol) ++ joins.foldMap(compileJoinAsFrom)
  }

  type JoinKind = String
  final case class Join(kind: JoinKind, withTable: TableName, withTableSymbol: String, onClause: Reference[Boolean])

  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }

  object Tuple2KK {
    implicit def functorK[A[_[_]]: FunctorK, B[_[_]]: FunctorK]: FunctorK[Tuple2KK[A, B, ?[_]]] = new FunctorK[Tuple2KK[A, B, ?[_]]] {
      def mapK[F[_], G[_]](af: Tuple2KK[A, B, F])(fk: F ~> G): Tuple2KK[A, B, G] = Tuple2KK(af.left.mapK(fk), af.right.mapK(fk))
    }
  }

  final case class GetSymbol(private val state: List[String]) {

    def next: (GetSymbol, String) =
      GetSymbol.getSymbol.run(state).map(_.leftMap(GetSymbol(_))).value
  }

  object GetSymbol {
    val initial = GetSymbol(Nil)
    private[GetSymbol] type SymbolST = State[List[String], String]

    private[GetSymbol] val getSymbol: SymbolST = State {
      case Nil => (List("a"), "a")
      case head :: t =>
        val newSymbol = (head.head + 1).toChar.toString
        (newSymbol :: head :: t, newSymbol)
    }
  }

  type JoinedTableQuery[A[_[_]], B[_[_]]] = TableQuery[Tuple2KK[A, B, ?[_]]]

  final case class TableQuery[A[_[_]]: FunctorK](base: QueryBase, lifted: A[Reference], private val getSymbol: GetSymbol) {

    def innerJoin[B[_[_]]: FunctorK](
      another: TableQuery[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): JoinedTableQuery[A, B] = join(another, "inner join")(onClause)

    def join[B[_[_]]: FunctorK](
      another: TableQuery[B],
      kind: JoinKind
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): JoinedTableQuery[A, B] = {
      val (newGetSymbol, anotherSymbol) = getSymbol.next
      TableQuery(
        base.copy(
          joins = (base.joins :+ Join(
            kind,
            another.base.table,
            anotherSymbol,
            onClause(
              lifted.mapK(setScope(base.tableSymbol)),
              another.lifted.mapK(setScope(anotherSymbol))
            )
          )) ++ another.base.joins
        ),
        Tuple2KK(
          lifted.mapK(setScope(base.tableSymbol)),
          another.lifted.mapK(setScope(anotherSymbol))
        ),
        newGetSymbol
      )
    }

    private def setScope(scope: String) = Reference.mapData {
      λ[ReferenceData ~> ReferenceData] {
        case ReferenceData.Column(n, None) =>
          ReferenceData.Column(n, Some(scope))
        case c @ ReferenceData.Column(_, Some(_)) =>
          println("ignoring already defined scope! " + c + ", " + scope)
          c
        case c => c
      }
    }

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(base, lifted.pure[SymbolState], selection, filters = Chain.empty)
  }

  def over[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr">" ++ _)

  def equal[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def equalOptionL[Type]: (Reference[Option[Type]], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def notNull[Type]: Reference[Option[Type]] => Reference[Boolean] =
    a =>
      Reference.Single(
        ReferenceData.Raw(a.compile.frag ++ fr"is not null"),
        Read[Boolean]
      )

  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"<>" ++ _)

  def binary[L, R](f: (Fragment, Fragment) => Fragment)(l: Reference[L], r: Reference[R]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(f(l.compile.frag, r.compile.frag)),
      Read[Boolean]
    )

  final case class Column(name: String) extends AnyVal

  sealed trait ReferenceData[Type] extends Product with Serializable {

    def widen[B >: Type]: ReferenceData[B] =
      this.asInstanceOf[ReferenceData[B]] //todo I'm pretty sure
  }

  object ReferenceData {
    final case class Column[A](col: datas.Column, scope: Option[String]) extends ReferenceData[A]
    final case class Lift[Type](value: Type, into: Param[Type HCons HNil]) extends ReferenceData[Type]
    final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]
  }

  sealed trait Reference[Type] extends Product with Serializable {

    def compile: TypedFragment[Type] =
      ReferenceCompiler.default.compileReference(this)
  }

  object Reference {
    final case class Single[Type](data: ReferenceData[Type], read: Read[Type]) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

    def lift[Type: Read](value: Type)(implicit param: Param[Type HCons HNil]): Reference[Type] =
      Reference.Single[Type](ReferenceData.Lift(value, param), Read[Type])

    def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
      λ[Reference ~> Reference] {
        case Single(data, read) => Single(fk(data), read)
        case Map(underlying, f) => Map(mapData(fk)(underlying), f)
        case Product(left, right) =>
          Product(mapData(fk)(left), mapData(fk)(right))
      }

    implicit val invariant: Apply[Reference] = new Apply[Reference] {
      def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)

      def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] =
        product(ff, fa).map { case (f, a) => f(a) }
      override def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] =
        Reference.Product(fa, fb)
    }
  }

  final case class TableSymbol(value: String) extends AnyVal

  type SymbolState[A] = State[Int, A]

  object SymbolState {
    val next: SymbolState[TableSymbol] = State.get[Int].map("x" + _).map(TableSymbol(_)) <* State.modify[Int](_ + 1)
  }

  final case class Query[A[_[_]], Queried](
    base: QueryBase,
    lifted: SymbolState[A[Reference]],
    selection: A[Reference] => Reference[Queried],
    filters: Chain[A[Reference] => Reference[Boolean]]
  ) {

    def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
      copy(filters = filters.append(filter))

    def compileSql: Query0[Queried] =
      lifted
        .map { lifted =>
          val compiledSelection = selection(lifted).compile

          implicit val read: Read[Queried] = compiledSelection.read

          val frag = fr"select" ++ compiledSelection.frag ++
            fr"from" ++ base.compileAsFrom ++
            Fragments.whereAnd(
              filters.map(_.apply(lifted).compile.frag).toList: _*
            )

          frag.query[Queried]
        }
        .runA(0)
        .value
  }

  trait ReferenceCompiler {
    def compileReference[Type](column: Reference[Type]): TypedFragment[Type]
  }

  final case class TypedFragment[Type](frag: Fragment, read: Read[Type]) {
    def map[B](f: Type => B): TypedFragment[B] = copy(read = read.map(f))

    def product[B](another: TypedFragment[B]): TypedFragment[(Type, B)] = {
      implicit val rl = read
      implicit val rr = another.read
      val _ = (rl, rr)
      TypedFragment(frag ++ fr"," ++ another.frag, Read[(Type, B)])
    }
  }

  object ReferenceCompiler {

    val default: ReferenceCompiler = new ReferenceCompiler {
      private def compileScoped[F[_]: Applicative, Type](reference: Reference[Type]): F[TypedFragment[Type]] =
        reference match {
          case Reference.Single(data, read) =>
            compileData[F, Type](read).apply(data)
          case m: Reference.Map[a, b] =>
            compileScoped[F, a](m.underlying).map(_.map(m.f))
          case p: Reference.Product[a, b] =>
            (compileScoped[F, a](p.left), compileScoped[F, b](p.right)).mapN(_ product _)
        }

      private def compileData[F[_]: Applicative, Type](read: Read[Type]): ReferenceData[Type] => F[TypedFragment[Type]] = {
        case ReferenceData.Column(column, scope) =>
          val scopeString = scope.foldMap(_ + ".")
          TypedFragment[Type](
            Fragment.const(scopeString + "\"" + column.name + "\""),
            read
          ).pure[F]
        case l: ReferenceData.Lift[a] =>
          implicit val param: Param[a HCons HNil] = l.into
          val _ = param //to make scalac happy
          TypedFragment[Type](fr"${l.value}", read).pure[F]
        case r: ReferenceData.Raw[a] =>
          TypedFragment[Type](r.fragment, read).pure[F]
      }

      def compileReference[Type](reference: Reference[Type]): TypedFragment[Type] =
        compileScoped[cats.Id, Type](reference)
    }
  }
}

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

object User {
  implicit val functorK: FunctorK[User] = new FunctorK[User] {

    def mapK[F[_], G[_]](af: User[F])(fk: F ~> G): User[G] =
      User(fk(af.id), fk(af.name), fk(af.age))
  }
}

final case class Book[F[_]](id: F[Long], userId: F[Long], parentId: F[Option[Long]])

object Book {
  implicit val functorK: FunctorK[Book] = new FunctorK[Book] {

    def mapK[F[_], G[_]](af: Book[F])(fk: F ~> G): Book[G] =
      Book(fk(af.id), fk(af.userId), fk(af.parentId))
  }
}

object Demo extends IOApp {
  import datas._

  import schemas.caseClassSchema
  import schemas.column

  val bookSchema: TableQuery[Book] =
    caseClassSchema(
      TableName("books"),
      (
        column[Long]("id"),
        column[Long]("user_id"),
        column[Option[Long]]("parent_id")
      ).mapN(Book[Reference])
    ) //types explicit, less typing

  val userSchema: TableQuery[User] =
    caseClassSchema(
      TableName("users"),
      Applicative[schemas.ST].map3(column[Long]("id"), column[String]("name"), column[Int]("age"))(
        User[Reference]
      ) //types automatically inferred
    )

  val qq2: TableQuery[Tuple2KK[Tuple2KK[User, Book, ?[_]], Book, ?[_]]] =
    userSchema.innerJoin(bookSchema)((u, b) => equal(u.id, b.userId)).innerJoin(bookSchema)((u, b) => equal(u.left.id, b.id))

  val q2 =
    qq2
      .select {
        _.asTuple.leftMap(_.asTuple) match {
          case ((user, book1), book2) =>
            (user.age, user.name, book1.userId, book2.id, user.id).tupled
        }
      }
      .where {
        _.asTuple.leftMap(_.asTuple) match {
          case ((user, _), _) => equal(user.age, Reference.lift(18))
        }
      }
      .where(t => equal(t.right.id, Reference.lift(1L)))

  val q3 =
    userSchema
      .innerJoin(bookSchema.innerJoin(bookSchema)((u, b) => equal(u.id, b.id)))(
        (u, b) => equal(u.id, b.left.userId)
      )
      .select {
        _.asTuple.map(_.asTuple) match {
          case (user, (book1, book2)) =>
            (user.age, user.name, book1.userId, book2.id, user.id).tupled
        }
      }
      .where {
        _.asTuple match {
          case (user, _) => equal(user.age, Reference.lift(18))
        }
      }
      .where(t => equal(t.right.right.id, Reference.lift(1L)))

  println(q3.compileSql)

  val a = {
    userSchema
      .innerJoin(bookSchema)((u, b) => equal(u.id, b.userId))
      .innerJoin(bookSchema) {
        case (Tuple2KK(_, b), bP) =>
          equal(b.parentId, bP.id.map(Option(_)))
      }
      .select(_.right.id)
  }

  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/postgres",
    "postgres",
    "postgres"
  )

  def getAll[A[_[_]], Queried](q: Query[A, Queried]): IO[Unit] =
    IO {
      println("\n\nstarting")
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain *> IO(println("\n\n"))

  def run(args: List[String]): IO[ExitCode] =
    getAll(q2) *> getAll(a) *> (if (false) getAll(q3) else IO.unit).as(ExitCode.Success)
}
