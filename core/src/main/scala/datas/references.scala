package datas

import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.~>
import cats.data.OptionT
import cats.Applicative
import cats.data.Nested
import scala.annotation.tailrec
import cats.InvariantSemigroupal
import cats.free.FreeInvariantMonoidal
import cats.InvariantMonoidal
import cats.Alternative
import cats.Monad

sealed trait ReferenceData[Type] extends Product with Serializable {

  def map[B](f: Type => B): ReferenceData[B] = this match {
    case ReferenceData.Lift(v)          => ReferenceData.Lift(f(v))
    case r @ ReferenceData.Column(_, _) => r.copy()
    case r @ ReferenceData.Raw(_)       => r.copy()
  }
}

object ReferenceData {
  final case class Column[A](col: schemas.Column, scope: Option[String]) extends ReferenceData[A]
  final case class Lift[Type](value: Type) extends ReferenceData[Type]
  final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]

  private[datas] def compileData[Type](metaType: Meta[Type]): ReferenceData[Type] => TypedFragment[Type] = {
    case ReferenceData.Column(column, scope) =>
      val scopeString = scope.foldMap(_ + ".")
      TypedFragment(Fragment.const(scopeString + column.showQuoted), metaType.get)

    case l: ReferenceData.Lift[a] =>
      implicit val putType: Put[a] = metaType.put
      val _ = putType //to make scalac happy
      TypedFragment(fr"${l.value}", metaType.get)

    case r: ReferenceData.Raw[a] => TypedFragment(r.fragment, metaType.get)
  }

  final case class TypedFragment[Type](fragment: Fragment, get: Get[Type])
}

sealed trait Reference[Type] extends Product with Serializable {
  private[datas] def compile: CompiledReference[Type] = Reference.compileReference(this)
  final def upcast: Reference[Type] = this
}

object Reference {

  final case class Single[Type](data: ReferenceData[Type], metaType: Meta[Type]) extends Reference[Type]
  final case class Optional[Type](underlying: Reference[Type]) extends Reference[Option[Type]]
  final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
  final case class Map[A, B](underlying: Reference[A], f: A => B, g: B => A) extends Reference[B]

  val liftOptionK: Reference ~> OptionT[Reference, ?] = λ[Reference ~> OptionT[Reference, ?]](r => OptionT(liftOption(r)))

  def liftOption[Type](reference: Reference[Type]): Reference[Option[Type]] = Optional(reference)

  def lift[Type: Get: Put](value: Type): Reference[Type] =
    Reference.Single[Type](ReferenceData.Lift(value), new Meta(Get[Type], Put[Type]))

  def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
    λ[Reference ~> Reference] {
      case Single(data, getType) => Single(fk(data), getType)
      case o: Optional[a]        => Optional(mapData(fk)(o.underlying))
      case Map(underlying, f, g) => Map(mapData(fk)(underlying), f, g)
      case Product(left, right)  => Product(mapData(fk)(left), mapData(fk)(right))
    }

  implicit val invariantSemigroupal: InvariantSemigroupal[Reference] = new InvariantSemigroupal[Reference] {
    def imap[A, B](fa: Reference[A])(f: A => B)(g: B => A): Reference[B] = Map(fa, f, g)

    def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Product(fa, fb)
  }

  private[datas] val compileReference: Reference ~> CompiledReference = λ[Reference ~> CompiledReference] {
    case Reference.Single(data, getType) =>
      val tf = ReferenceData.compileData(getType)(data)
      CompiledReference(tf.fragment, CompiledGet.JustGet(tf.get).freed)

    case r: Reference.Optional[a] =>
      val result = compileReference(r.underlying)

      CompiledReference(result.frag, result.get.foldMap(CompiledGet.liftOption).value)

    case m: Reference.Map[a, b] =>
      m.underlying match {
        case Reference.Single(data, meta) =>
          compileReference(Reference.Single(data.map(m.f), meta.imap(m.f)(m.g)))
        case m2: Reference.Map[c, d] =>
          compileReference(Reference.Map(m2.underlying, m2.f andThen m.f, m.g andThen m2.g))
        case other => compileReference(other).imap(m.f)(m.g)
      }

    case p: Reference.Product[a, b] => (compileReference(p.left), compileReference(p.right)).tupled
  }
}

final case class CompiledReference[A](frag: Fragment, get: CompiledGet.Freed[A])

object CompiledReference {

  implicit val invariantSemigroupal: InvariantSemigroupal[CompiledReference] = new InvariantSemigroupal[CompiledReference] {

    def product[A, B](fa: CompiledReference[A], fb: CompiledReference[B]): CompiledReference[(A, B)] =
      CompiledReference(fa.frag ++ fr0"," ++ fb.frag, (fb.get, fa.get).tupled.imap(_.swap)(_.swap))

    def imap[A, B](fa: CompiledReference[A])(f: A => B)(g: B => A): CompiledReference[B] = CompiledReference(fa.frag, fa.get.imap(f)(g))
  }
}

sealed trait CompiledGet[A] extends Product with Serializable {
  final def freed: CompiledGet.Freed[A] = FreeInvariantMonoidal.lift(this)
}

object CompiledGet {

  final case class JustGet[A](underlying: Get[A]) extends CompiledGet[A]

  final case class NullableGet[A](underlying: CompiledGet[A]) extends CompiledGet[Option[A]]

  val toRead: CompiledGet ~> Read = {
    @tailrec
    def readNullable[a, b](get: CompiledGet[a])(f: Option[a] => Option[b]): Read[Option[b]] = get match {
      case JustGet(underlying) => Read.fromGetOption(underlying).map(f)
      case ng: NullableGet[c]  => readNullable(ng.underlying)(a => f(a.map(_.some)))
    }

    λ[CompiledGet ~> Read] {
      case JustGet(a)         => Read.fromGet(a)
      case ng: NullableGet[a] => readNullable(ng.underlying)(identity)
    }
  }

  implicit val applicativeRead: Applicative[Read] = new Applicative[Read] {
    def ap[A, B](ff: Read[A => B])(fa: Read[A]): Read[B] = fa.ap(ff)
    def pure[A](x: A): Read[A] = Read.unit.map(_ => x)
  }

  //todo this definitely needs law testing!
  implicit def invariantMonoidalNested[F[_]: InvariantMonoidal, G[_]: Alternative: Monad]: InvariantMonoidal[Nested[F, G, *]] =
    new InvariantMonoidal[Nested[F, G, *]] {

      def product[A, B](fa: Nested[F, G, A], fb: Nested[F, G, B]): Nested[F, G, (A, B)] =
        (fa.value, fb.value).imapN((_, _).tupled(Monad[G], Monad[G]))(_.separate).nested

      def imap[A, B](fa: Nested[F, G, A])(f: A => B)(g: B => A): Nested[F, G, B] =
        fa.value.imap(Monad[G].map(_)(f))(b => Monad[G].map(b)(g)).nested

      val unit: Nested[F, G, Unit] = InvariantMonoidal[F].unit.imap(_ => Alternative[G].unit)(_ => ()).nested
    }

  val liftOption: CompiledGet ~> Nested[Freed, Option, *] =
    λ[CompiledGet ~> Nested[Freed, Option, *]](NullableGet(_).freed.nested)

  type Freed[A] = FreeInvariantMonoidal[CompiledGet, A]
}
