package datas

import cats.tagless.FunctorK
import cats.arrow.FunctionK
import cats.~>
import cats.data.OptionT
import cats.tagless.implicits._
import simulacrum.typeclass
import cats.implicits._
import cats.InvariantSemigroupal

object tagless {

  /**
    * A higher-kinded Traverse that uses InvariantSemigroupal instead of Apply, hence the "Invariant" prefix.
    */
  @typeclass
  trait InvariantTraverseK[Alg[_[_]]] extends FunctorK[Alg] {

    def itraverseK[F[_], G[_]: InvariantSemigroupal, H[_]](alg: Alg[F])(fk: F ~> λ[a => G[H[a]]]): G[Alg[H]]

    def isequenceK[F[_]: InvariantSemigroupal, G[_]](alg: Alg[λ[a => F[G[a]]]]): F[Alg[G]] =
      itraverseK[λ[a => F[G[a]]], F, G](alg)(FunctionK.id[λ[a => F[G[a]]]])

    override def mapK[F[_], G[_]](af: Alg[F])(fk: F ~> G): Alg[G] = itraverseK[F, cats.Id, G](af)(fk)

    /**
      * Like [[isequenceK]], but with the second effect hardcoded to [[cats.Id]] for better inference.
      * */
    def isequenceKId[F[_]: InvariantSemigroupal](alg: Alg[F]): F[Alg[cats.Id]] = isequenceK[F, cats.Id](alg)
  }

  // An option transformer for higher-kinded types
  final case class OptionTK[F[_[_]], G[_]](underlying: F[OptionT[G, ?]]) extends AnyVal

  object OptionTK {
    def liftK[F[_[_]]: FunctorK, G[_]](fg: F[G])(lift: G ~> OptionT[G, ?]): OptionTK[F, G] = OptionTK(fg.mapK(lift))

    import InvariantTraverseK.ops._

    //TraverseK for OptionT
    private implicit def optionTTraverseK[A]: InvariantTraverseK[OptionT[*[_], A]] = new InvariantTraverseK[OptionT[*[_], A]] {

      def itraverseK[F[_], G[_]: InvariantSemigroupal, H[_]](alg: OptionT[F, A])(fk: F ~> λ[a => G[H[a]]]): G[OptionT[H, A]] =
        fk(alg.value).imap(OptionT(_))(_.value)
    }

    //TraverseK for OptionTK
    implicit def itraverseK[F[_[_]]: InvariantTraverseK]: InvariantTraverseK[OptionTK[F, *[_]]] =
      new InvariantTraverseK[OptionTK[F, *[_]]] {

        def itraverseK[G[_], H[_]: InvariantSemigroupal, I[_]](alg: OptionTK[F, G])(fk: G ~> λ[a => H[I[a]]]): H[OptionTK[F, I]] =
          alg
            .underlying
            .itraverseK[H, OptionT[I, *]] {
              λ[OptionT[G, *] ~> λ[a => H[OptionT[I, a]]]] {
                case fa: OptionT[G, a] => optionTTraverseK[a].itraverseK(fa)(fk)
              }
            }
            .imap(OptionTK(_))(_.underlying)
      }
  }

  // A tuple2 of algebras in the same effect.
  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }

  object Tuple2KK {

    import InvariantTraverseK.ops._

    implicit def itraverseK[A[_[_]]: InvariantTraverseK, B[_[_]]: InvariantTraverseK]: InvariantTraverseK[Tuple2KK[A, B, *[_]]] =
      new InvariantTraverseK[Tuple2KK[A, B, *[_]]] {

        def itraverseK[F[_], G[_]: InvariantSemigroupal, H[_]](alg: Tuple2KK[A, B, F])(fk: F ~> λ[a => G[H[a]]]): G[Tuple2KK[A, B, H]] =
          (alg.left.itraverseK(fk), alg.right.itraverseK(fk)).imapN(Tuple2KK(_, _))(a => (a.left, a.right))
      }
  }
}
