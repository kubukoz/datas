import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.data.State
import cats.tagless.FunctorK
import cats.~>
import cats.tagless.implicits._
import cats.Apply
import shapeless.HNil
import shapeless.{:: => HCons}
import cats.mtl.MonadState
import cats.mtl.instances.all._
import cats.FlatMap

object datas {
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
            TableQuery(QueryBase.FromTable(name, data, FunctorK[F]))
        }
        .value
  }

  final case class TableName(name: String) extends AnyVal {
    def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  sealed trait QueryBase[A[_[_]]] extends Product with Serializable {

    def compileAsFrom: (Fragment, A[Reference]) =
      QueryBase.doCompile[A, State[Int, *]](this).runA(0).value
  }

  type IndexState[F[_]] = MonadState[F, Int]

  object IndexState {
    def apply[F[_]](implicit F: IndexState[F]): IndexState[F] = F
    def getAndInc[F[_]: IndexState: Apply]: F[Int] = IndexState[F].get <* IndexState[F].modify(_ + 1)
  }

  object QueryBase {
    final case class FromTable[A[_[_]]](table: TableName, lifted: A[Reference], functorK: FunctorK[A]) extends QueryBase[A]
    final case class Join[A[_[_]], B[_[_]]](
      left: QueryBase[A],
      right: QueryBase[B],
      kind: JoinKind,
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ) extends QueryBase[Tuple2KK[A, B, *[_]]]

    /**
      * Returns: the compiled query base (from + joins) and the scoped references underlying it (passed later to selections and filters).
      */
    def doCompile[A[_[_]], F[_]: IndexState: FlatMap](qbase: QueryBase[A]): F[(Fragment, A[Reference])] =
      qbase match {
        case t: FromTable[t] =>
          implicit val functorK = t.functorK
          IndexState.getAndInc[F].map { index =>
            val scope = t.table.name + "_x" + index
            (
              t.table.identifierFragment ++ Fragment.const(scope),
              t.lifted.mapK(setScope(scope))
            )
          }

        case j: Join[a, b] =>
          import j._
          (doCompile[a, F](left), doCompile[b, F](right)).mapN {
            case ((leftFrag, leftCompiledReference), (rightFrag, rightCompiledReference)) =>
              val thisJoinClause = onClause(leftCompiledReference, rightCompiledReference).compile.frag

              val joinFrag = leftFrag ++ Fragment.const(kind) ++ rightFrag ++ fr"on" ++ thisJoinClause

              (joinFrag, Tuple2KK(leftCompiledReference, rightCompiledReference))
          }
      }

  }

  type JoinKind = String

  type JoinedTableQuery[A[_[_]], B[_[_]]] = TableQuery[Tuple2KK[A, B, ?[_]]]

  //todo this class is redundant, should be merged with querybase next
  final case class TableQuery[A[_[_]]: FunctorK](base: QueryBase[A]) {

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
    ): JoinedTableQuery[A, B] =
      TableQuery(
        QueryBase.Join(base, another.base, kind, onClause)
      )

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(base, selection, filters = Chain.empty)
  }

  private def setScope(scope: String) = Reference.mapData {
    λ[ReferenceData ~> ReferenceData] {
      case ReferenceData.Column(n, None) =>
        ReferenceData.Column(n, Some(scope))
      case c @ ReferenceData.Column(_, Some(_)) =>
        //todo this case is impossible, we should have that in the types
        //or else inline it with the catch-all below
        c
      case c => c
    }
  }

  final case class Column(name: String) extends AnyVal

  sealed trait ReferenceData[Type] extends Product with Serializable {

    def widen[B >: Type]: ReferenceData[B] =
      this.asInstanceOf[ReferenceData[B]] //todo I'm pretty sure it won't work for some cases
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

  final case class Query[A[_[_]], Queried](
    base: QueryBase[A],
    selection: A[Reference] => Reference[Queried],
    filters: Chain[A[Reference] => Reference[Boolean]]
  ) {

    def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
      copy(filters = filters.append(filter))

    def compileSql: Query0[Queried] = {
      val (compiledFrom, compiledReference) = base.compileAsFrom

      val compiledSelection = selection(compiledReference).compile

      implicit val read: Read[Queried] = compiledSelection.read

      val frag = fr"select" ++ compiledSelection.frag ++
        fr"from" ++ compiledFrom ++
        Fragments.whereAnd(
          filters.map(_.apply(compiledReference).compile.frag).toList: _*
        )

      frag.query[Queried]
    }
  }

  trait ReferenceCompiler {
    def compileReference: Reference ~> TypedFragment
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
      private def compileData[Type](read: Read[Type]): ReferenceData[Type] => TypedFragment[Type] = {
        case ReferenceData.Column(column, scope) =>
          val scopeString = scope.foldMap(_ + ".")
          TypedFragment[Type](
            Fragment.const(scopeString + "\"" + column.name + "\""),
            read
          )
        case l: ReferenceData.Lift[a] =>
          implicit val param: Param[a HCons HNil] = l.into
          val _ = param //to make scalac happy
          TypedFragment[Type](fr"${l.value}", read)
        case r: ReferenceData.Raw[a] =>
          TypedFragment[Type](r.fragment, read)
      }

      val compileReference: Reference ~> TypedFragment = λ[Reference ~> TypedFragment] {
        case Reference.Single(data, read) => compileData(read)(data)
        case m: Reference.Map[a, b]       => compileReference(m.underlying).map(m.f)
        case p: Reference.Product[a, b]   => compileReference(p.left) product compileReference(p.right)
      }
    }
  }

  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }

  object Tuple2KK {
    implicit def functorK[A[_[_]]: FunctorK, B[_[_]]: FunctorK]: FunctorK[Tuple2KK[A, B, ?[_]]] = new FunctorK[Tuple2KK[A, B, ?[_]]] {
      def mapK[F[_], G[_]](af: Tuple2KK[A, B, F])(fk: F ~> G): Tuple2KK[A, B, G] = Tuple2KK(af.left.mapK(fk), af.right.mapK(fk))
    }
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
}
