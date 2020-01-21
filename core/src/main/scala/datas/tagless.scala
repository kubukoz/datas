package datas

import cats.tagless.FunctorK
import cats.Apply
import cats.arrow.FunctionK
import cats.~>
import cats.data.OptionT
import cats.tagless.implicits._
import simulacrum.typeclass
import cats.implicits._

object tagless {

  @typeclass
  trait TraverseK[Alg[_[_]]] extends FunctorK[Alg] {

    def traverseK[F[_], G[_]: Apply, H[_]](alg: Alg[F])(fk: F ~> λ[a => G[H[a]]]): G[Alg[H]]

    def sequenceK[F[_]: Apply, G[_]](alg: Alg[λ[a => F[G[a]]]]): F[Alg[G]] =
      traverseK[λ[a => F[G[a]]], F, G](alg)(FunctionK.id[λ[a => F[G[a]]]])

    override def mapK[F[_], G[_]](af: Alg[F])(fk: F ~> G): Alg[G] = traverseK[F, cats.Id, G](af)(fk)

    /**
      * Like [[sequenceK]], but with the second effect hardcoded to [[cats.Id]] for better inference.
      * */
    def sequenceKId[F[_]: Apply](alg: Alg[F]): F[Alg[cats.Id]] = sequenceK[F, cats.Id](alg)
  }

  // An option transformer for higher-kinded types
  final case class OptionTK[F[_[_]], G[_]](underlying: F[OptionT[G, ?]]) extends AnyVal

  object OptionTK {
    def liftK[F[_[_]]: FunctorK, G[_]](fg: F[G])(lift: G ~> OptionT[G, ?]): OptionTK[F, G] = OptionTK(fg.mapK(lift))

    import TraverseK.ops._

    //TraverseK for OptionT
    private implicit def optionTTraverseK[A]: TraverseK[OptionT[*[_], A]] = new TraverseK[OptionT[*[_], A]] {
      def traverseK[F[_], G[_]: Apply, H[_]](alg: OptionT[F, A])(fk: F ~> λ[a => G[H[a]]]): G[OptionT[H, A]] = fk(alg.value).map(OptionT(_))
    }

    //TraverseK for OptionTK
    implicit def traverseK[F[_[_]]: TraverseK]: TraverseK[OptionTK[F, *[_]]] = new TraverseK[OptionTK[F, *[_]]] {

      def traverseK[G[_], H[_]: Apply, I[_]](alg: OptionTK[F, G])(fk: G ~> λ[a => H[I[a]]]): H[OptionTK[F, I]] =
        alg
          .underlying
          .traverseK[H, OptionT[I, *]] {
            new (OptionT[G, *] ~> λ[a => H[OptionT[I, a]]]) {
              def apply[A](fa: OptionT[G, A]): H[OptionT[I, A]] =
                optionTTraverseK[A].traverseK(fa)(fk)
            }
          }
          .map(OptionTK(_))
    }
  }

  // A tuple2 of higher-kinded types.
  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }
}
