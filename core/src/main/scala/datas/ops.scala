package datas

import doobie._
import doobie.implicits._

object ops {

  implicit final class ReferenceOps[Type](private val self: Reference[Type]) extends AnyVal {
    def >=(another: Reference[Type]): Reference[Boolean] = over(self, another)
    def <=(another: Reference[Type]): Reference[Boolean] = another >= self
    def ===(another: Reference[Type]): Reference[Boolean] = equal(self, another)

    def ||(another: Reference[Boolean])(implicit isBool: Type =:= Boolean): Reference[Boolean] = {
      val _ = isBool
      binary(_ ++ fr"or" ++ _)(self, another)
    }
  }

  implicit final class LiftAny[Type](private val self: Type) extends AnyVal {

    def liftReference(implicit getType: Get[Type], putType: Put[Type]): Reference[Type] =
      Reference.lift(self)
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
      Meta[Boolean]
    )

  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"<>" ++ _)

  def binary[L, R](f: (Fragment, Fragment) => Fragment)(l: Reference[L], r: Reference[R]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(f(l.compile.frag, r.compile.frag)),
      Meta[Boolean]
    )
}
