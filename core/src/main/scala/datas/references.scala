package datas
import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.~>
import cats.Apply
import shapeless.HNil
import shapeless.{:: => HCons}
import cats.data.OptionT

sealed trait ReferenceData[Type] extends Product with Serializable

object ReferenceData {
  final case class Column[A](col: schemas.Column, scope: Option[String]) extends ReferenceData[A]
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

    case m: Reference.Map[a, b] => compileReference(m.underlying).map(m.f)

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

private[datas] final case class CompiledReference[Type](frag: Fragment, readOrGet: Either[Get[Type], Read[Type]]) {
  def rmap[T2](f: Either[Get[Type], Read[Type]] => T2): (Fragment, T2) = (frag, f(readOrGet))

  def ermap[T2](f: Either[Get[Type], Read[Type]] => Either[Get[T2], Read[T2]]): CompiledReference[T2] =
    CompiledReference(frag, f(readOrGet))

  def map[T2](f: Type => T2): CompiledReference[T2] = ermap(_.bimap(_.map(f), _.map(f)))
}
