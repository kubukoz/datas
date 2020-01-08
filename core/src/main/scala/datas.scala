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
import cats.data.OptionT

object datas {

  trait SequenceK[Alg[_[_]]] {
    def sequence[F[_]: Apply, G[_]](alg: Alg[λ[a => F[G[a]]]]): F[Alg[G]]
  }

  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]
    type STRef[X] = ST[Reference[X]]

    def column[Type: Get](name: String): STRef[Type] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Single(ReferenceData.Column(col, None), Get[Type]))
    }

    def caseClassSchema[F[_[_]]: FunctorK: SequenceK](name: TableName, stClass: F[STRef]): TableQuery[F] =
      implicitly[SequenceK[F]]
        .sequence(stClass)
        .runA(Chain.empty)
        .map(w => TableQuery.FromTable(name, w, FunctorK[F], implicitly[SequenceK[F]].sequence[Reference, cats.Id]))
        .value
  }

  final case class TableName(name: String) extends AnyVal {
    private[datas] def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  private[datas] type IndexState[F[_]] = MonadState[F, Int]

  private[datas] object IndexState {
    def apply[F[_]](implicit F: IndexState[F]): IndexState[F] = F
    def getAndInc[F[_]: IndexState: Apply]: F[Int] = IndexState[F].get <* IndexState[F].modify(_ + 1)
  }

  sealed trait TableQuery[A[_[_]]] extends Product with Serializable {

    def innerJoin[B[_[_]]](
      another: TableQuery[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): TableQuery[JoinKind.Inner[A, B]#Out] =
      join(another)(_.inner)(onClause)

    def leftJoin[B[_[_]]: FunctorK](
      another: TableQuery[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): TableQuery[JoinKind.Left[A, B]#Out] =
      join(another)(_.left)(onClause)

    def join[B[_[_]], Joined[_[_]]](
      another: TableQuery[B]
    )(
      kindF: JoinKind.type => JoinKind[A, B, Joined]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): TableQuery[Joined] =
      TableQuery.Join(this, another, kindF(JoinKind), onClause)

    def selectAll: Query[A, A[cats.Id]] = select(aref => this.asInstanceOf[TableQuery.FromTable[A]].unwrap(aref))

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(this, selection, filters = Chain.empty)
  }

  private[datas] object TableQuery {
    final case class FromTable[A[_[_]]](
      table: TableName,
      lifted: A[Reference],
      functorK: FunctorK[A],
      unwrap: A[Reference] => Reference[A[cats.Id]]
    ) extends TableQuery[A]
    final case class Join[A[_[_]], B[_[_]], Joined[_[_]]](
      left: TableQuery[A],
      right: TableQuery[B],
      kind: JoinKind[A, B, Joined],
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ) extends TableQuery[Joined]

    /**
      * Returns: the compiled query base (from + joins) and the scoped references underlying it (passed later to selections and filters).
      */
    def compileQuery[A[_[_]], F[_]: IndexState: FlatMap]: TableQuery[A] => F[(Fragment, A[Reference])] = {
      case t: FromTable[t] =>
        implicit val functorK = t.functorK
        IndexState.getAndInc[F].map { index =>
          val scope = t.table.name + "_x" + index
          (
            t.table.identifierFragment ++ Fragment.const(scope),
            t.lifted.mapK(setScope(scope))
          )
        }

      case j: Join[a, b, k] =>
        import j._
        (compileQuery[a, F].apply(left), compileQuery[b, F].apply(right)).mapN {
          case ((leftFrag, leftCompiledReference), (rightFrag, rightCompiledReference)) =>
            val thisJoinClause = onClause(leftCompiledReference, rightCompiledReference).compile.frag

            val joinFrag = leftFrag ++ Fragment.const(kind.kind) ++ rightFrag ++ fr"on" ++ thisJoinClause

            (joinFrag, kind.buildJoined(leftCompiledReference, rightCompiledReference))
        }
    }

  }

  trait JoinKind[A[_[_]], B[_[_]], Joined[_[_]]] {
    final type Out[F[_]] = Joined[F]

    def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference]
    def kind: String
  }

  object JoinKind {
    type Inner[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, B, ?[_]]]
    type Left[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, OptionTK[B, ?[_]], ?[_]]]

    def left[A[_[_]], B[_[_]]: FunctorK]: Left[A, B] = make("left join")((a, b) => Tuple2KK(a, OptionTK.liftK(b)(Reference.liftOptionK)))
    def inner[A[_[_]], B[_[_]]]: Inner[A, B] = make("inner join")(Tuple2KK.apply _)

    private def make[A[_[_]], B[_[_]], Joined[_[_]]](
      name: String
    )(
      build: (A[Reference], B[Reference]) => Joined[Reference]
    ): JoinKind[A, B, Joined] = new JoinKind[A, B, Joined] {
      def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference] = build(a, b)
      val kind: String = name
    }
  }

  private def setScope(scope: String): Reference ~> Reference = Reference.mapData {
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

  final case class Column(name: String) extends AnyVal {
    def showQuoted: String = "\"" + name + "\""
  }

  sealed trait ReferenceData[Type] extends Product with Serializable

  object ReferenceData {
    final case class Column[A](col: datas.Column, scope: Option[String]) extends ReferenceData[A]
    final case class Lift[Type](value: Type, into: Param[Type HCons HNil]) extends ReferenceData[Type]
    final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]

    private[datas] def compileData[Type](getType: Get[Type]): ReferenceData[Type] => TypedFragment[Type] = {
      case ReferenceData.Column(column, scope) =>
        val scopeString = scope.foldMap(_ + ".")
        TypedFragment(Fragment.const(scopeString + column.showQuoted), getType)
      case l: ReferenceData.Lift[a] =>
        implicit val param: Param[a HCons HNil] = l.into
        val _ = param //to make scalac happy
        TypedFragment(fr"${l.value}", getType)
      case r: ReferenceData.Raw[a] => TypedFragment(r.fragment, getType)
    }

    final case class TypedFragment[Type](fragment: Fragment, get: Get[Type])
  }

  sealed trait Reference[Type] extends Product with Serializable {

    private[datas] def compile: CompiledReference[Type] =
      Reference.compileReference(this)
  }

  object Reference {
    final case class Optional[Type](underlying: Reference[Type]) extends Reference[Option[Type]]
    final case class Single[Type](data: ReferenceData[Type], getType: Get[Type]) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

    val liftOptionK: Reference ~> OptionT[Reference, ?] = λ[Reference ~> OptionT[Reference, ?]](r => OptionT(Optional(r)))

    def liftOption[Type](reference: Reference[Type]): Reference[Option[Type]] = liftOptionK(reference).value

    def lift[Type: Get](value: Type)(implicit param: Param[Type HCons HNil]): Reference[Type] =
      Reference.Single[Type](ReferenceData.Lift(value, param), Get[Type])

    def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
      λ[Reference ~> Reference] {
        case Single(data, getType) => Single(fk(data), getType)
        case Optional(underlying)  => Optional(mapData(fk)(underlying))
        case Map(underlying, f)    => Map(mapData(fk)(underlying), f)
        case Product(left, right)  => Product(mapData(fk)(left), mapData(fk)(right))
      }

    implicit val apply: Apply[Reference] = new Apply[Reference] {
      override def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)
      override def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] = product(ff, fa).map { case (f, a) => f(a) }
      override def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Reference.Product(fa, fb)
    }

    private[datas] val compileReference: Reference ~> CompiledReference = λ[Reference ~> CompiledReference] {
      case Reference.Single(data, getType) =>
        val tf = ReferenceData.compileData(getType)(data)
        CompiledReference(tf.fragment, tf.get.asLeft)

      case r: Reference.Optional[a] =>
        compileReference(r.underlying).ermap {
          case Left(get)   => Read.fromGetOption(get).asRight
          case Right(read) => read.map(_.some).asRight
        }

      case m: Reference.Map[a, b] => compileReference(m.underlying).mapBoth(m.f)

      case p: Reference.Product[a, b] =>
        def toRead[A]: Either[Get[A], Read[A]] => Read[A] = _.fold(Read.fromGet(_), identity)

        val (leftFrag, leftRead) = compileReference(p.left).rmap(toRead)
        val (rightFrag, rightRead) = compileReference(p.right).rmap(toRead)

        implicit val lR = leftRead
        implicit val rR = rightRead

        val _ = (lR, rR) //making scalac happy

        CompiledReference(leftFrag ++ fr", " ++ rightFrag, Read[(a, b)].asRight)
    }
  }

  final case class Query[A[_[_]], Queried](
    base: TableQuery[A],
    selection: A[Reference] => Reference[Queried],
    filters: Chain[A[Reference] => Reference[Boolean]]
  ) {

    def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
      copy(filters = filters.append(filter))

    def compileSql: Query0[Queried] = {
      val (compiledQueryBase, compiledReference) = TableQuery.compileQuery[A, State[Int, *]].apply(base).runA(0).value

      val compiledSelection = selection(compiledReference).compile

      val frag = fr"select" ++ compiledSelection.frag ++
        fr"from" ++ compiledQueryBase ++
        Fragments.whereAnd(
          filters.map(_.apply(compiledReference).compile.frag).toList: _*
        )

      implicit val read: Read[Queried] = compiledSelection.readOrGet.fold(Read.fromGet(_), identity)

      frag.query[Queried]
    }
  }

  private[datas] final case class CompiledReference[Type](frag: Fragment, readOrGet: Either[Get[Type], Read[Type]]) {
    def rmap[T2](f: Either[Get[Type], Read[Type]] => T2): (Fragment, T2) = (frag, f(readOrGet))

    def ermap[T2](f: Either[Get[Type], Read[Type]] => Either[Get[T2], Read[T2]]): CompiledReference[T2] =
      CompiledReference(frag, f(readOrGet))

    def mapBoth[T2](f: Type => T2): CompiledReference[T2] = ermap(_.bimap(_.map(f), _.map(f)))
  }

  //A ((*->*)->*)->* (or so)-kinded option transformer
  case class OptionTK[F[_[_]], G[_]](underlying: F[OptionT[G, ?]])

  object OptionTK {
    def liftK[F[_[_]]: FunctorK, G[_]](fg: F[G])(lift: G ~> OptionT[G, ?]): OptionTK[F, G] = OptionTK(fg.mapK(lift))
  }

  //A tuple2 of even-higher-kinded types.
  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }

  def over[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr">" ++ _)

  def equal[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def equalOptionL[Type]: (Reference[Option[Type]], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def notNull[Type](a: Reference[Option[Type]]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(a.compile.frag ++ fr"is not null"),
      Get[Boolean]
    )

  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"<>" ++ _)

  def binary[L, R](f: (Fragment, Fragment) => Fragment)(l: Reference[L], r: Reference[R]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(f(l.compile.frag, r.compile.frag)),
      Get[Boolean]
    )
}
