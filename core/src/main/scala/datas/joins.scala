package datas

import datas.tagless.Tuple2KK
import datas.tagless.OptionTK
import datas.tagless.TraverseK
import cats.tagless.FunctorK

sealed trait JoinKind[A[_[_]], B[_[_]], Joined[_[_]]] {
  final type Out[F[_]] = Joined[F]

  // ◙‿◙
  private[datas] def buildJoint(a: A[Reference], b: B[Reference]): Joined[Reference]
  private[datas] def kind: String
  private[datas] def deriveTraverseK(left: TraverseK[A], right: TraverseK[B]): TraverseK[Joined]
}

object JoinKind {
  type Inner[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, B, ?[_]]]
  type Left[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, OptionTK[B, ?[_]], ?[_]]]

  def left[A[_[_]], B[_[_]]: FunctorK]: Left[A, B] =
    make[A, B, Left[A, B]#Out]("left join")((a, b) => Tuple2KK(a, OptionTK.liftK(b)(Reference.liftOptionK)))(implicit a =>
      implicit b => TraverseK[Left[A, B]#Out]
    )

  def inner[A[_[_]], B[_[_]]]: Inner[A, B] =
    make[A, B, Inner[A, B]#Out]("inner join")(Tuple2KK.apply _)(implicit a => implicit b => TraverseK[Inner[A, B]#Out])

  private def make[A[_[_]], B[_[_]], Joined[_[_]]](
    name: String
  )(
    build: (A[Reference], B[Reference]) => Joined[Reference]
  )(
    deriveTraverseKFromParts: TraverseK[A] => TraverseK[B] => TraverseK[Joined]
  ): JoinKind[A, B, Joined] = new JoinKind[A, B, Joined] {
    def buildJoint(a: A[Reference], b: B[Reference]): Joined[Reference] = build(a, b)
    val kind: String = name

    def deriveTraverseK(left: TraverseK[A], right: TraverseK[B]): TraverseK[Joined] = deriveTraverseKFromParts(left)(right)
  }
}
