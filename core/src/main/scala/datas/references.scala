package datas

import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.~>
import cats.Apply
import cats.data.OptionT
import cats.Applicative
import cats.free.FreeApplicative
import cats.data.Nested
import scala.annotation.tailrec

sealed trait ReferenceData[Type] extends Product with Serializable

object ReferenceData {
  final case class Column[A](col: schemas.Column, scope: Option[String]) extends ReferenceData[A]
  final case class Lift[Type](value: Type, put: Put[Type]) extends ReferenceData[Type]
  final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]

  private[datas] def compileData[Type](getType: Get[Type]): ReferenceData[Type] => TypedFragment[Type] = {
    case ReferenceData.Column(column, scope) =>
      val scopeString = scope.foldMap(_ + ".")
      TypedFragment(Fragment.const(scopeString + column.showQuoted), getType)

    case l: ReferenceData.Lift[a] =>
      implicit val putType: Put[a] = l.put
      val _ = putType // to make scalac happy
      TypedFragment(fr"${l.value}", getType)

    case r: ReferenceData.Raw[a] => TypedFragment(r.fragment, getType)
  }

  final case class TypedFragment[Type](fragment: Fragment, get: Get[Type])
}

sealed trait Reference[Type] extends Product with Serializable {
  private[datas] def compile: CompiledReference[Type] = Reference.compileReference(this)
  final def upcast: Reference[Type] = this
}

object Reference {

  final case class Single[Type](data: ReferenceData[Type], getType: Get[Type]) extends Reference[Type]
  final case class Optional[Type](underlying: Reference[Type]) extends Reference[Option[Type]]
  final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
  final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

  val liftOptionK: Reference ~> OptionT[Reference, *] = λ[Reference ~> OptionT[Reference, *]](r => OptionT(liftOption(r)))

  def liftOption[Type](reference: Reference[Type]): Reference[Option[Type]] = Optional(reference)

  def lift[Type: Get: Put](value: Type): Reference[Type] =
    Reference.Single[Type](ReferenceData.Lift(value, Put[Type]), Get[Type])

  def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
    λ[Reference ~> Reference] {
      case Single(data, getType) => Single(fk(data), getType)
      case o: Optional[a]        => Optional(mapData(fk)(o.underlying))
      case Map(underlying, f)    => Map(mapData(fk)(underlying), f)
      case Product(left, right)  => Product(mapData(fk)(left), mapData(fk)(right))
    }

  implicit val apply: Apply[Reference] = new Apply[Reference] {
    override def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)

    override def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] =
      Reference.Product(ff, fa).upcast.map { case (f, a) => f(a) }
  }

  private[datas] val compileReference: Reference ~> CompiledReference = λ[Reference ~> CompiledReference] {
    case Reference.Single(data, getType) =>
      val tf = ReferenceData.compileData(getType)(data)
      CompiledReference(tf.fragment, CompiledGet.JustGet(tf.get).freed)

    case r: Reference.Optional[a] =>
      val result = compileReference(r.underlying)

      CompiledReference(result.frag, result.get.foldMap(CompiledGet.liftOption).value)

    case m: Reference.Map[a, b] => compileReference(m.underlying).map(m.f)

    case p: Reference.Product[a, b] => (compileReference(p.left), compileReference(p.right)).tupled
  }

}

final case class CompiledReference[A](frag: Fragment, get: CompiledGet.Freed[A])

object CompiledReference {

  implicit val apply: Apply[CompiledReference] = new Apply[CompiledReference] {
    def map[A, B](fa: CompiledReference[A])(f: A => B): CompiledReference[B] = CompiledReference(fa.frag, fa.get.map(f))

    def ap[A, B](ff: CompiledReference[A => B])(fa: CompiledReference[A]): CompiledReference[B] =
      // Ordering matters - `fa` needs to be sequenced before `ff`, but the first fragment comes from `ff` (in `product`).
      CompiledReference(ff.frag ++ fr0"," ++ fa.frag, (ff.get, fa.get).mapN((f, a) => f(a)))
    // CompiledReference(ff.frag ++ fr0"," ++ fa.frag, (fa.get, ff.get).mapN((a, f) => f(a)))

  }

}

sealed trait CompiledGet[A] extends Product with Serializable {
  final def freed: CompiledGet.Freed[A] = FreeApplicative.lift(this)
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

  val liftOption: CompiledGet ~> Nested[Freed, Option, *] =
    λ[CompiledGet ~> Nested[Freed, Option, *]](NullableGet(_).freed.nested)

  type Freed[A] = FreeApplicative[CompiledGet, A]
}
