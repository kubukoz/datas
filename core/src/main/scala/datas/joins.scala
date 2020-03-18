package datas

import datas.tagless.Tuple2KK
import datas.tagless.OptionTK
import datas.tagless.InvariantTraverseK

sealed trait JoinKind[A[_[_]], B[_[_]], Joined[_[_]]] {
  final type Out[F[_]] = Joined[F]

  // ◙‿◙
  private[datas] def buildJoint(a: A[Reference], b: B[Reference]): Joined[Reference]
  private[datas] def kind: String
  //todo should this be inlined? We need some constraints anyway and the instance should have all of them too
  private[datas] def itraverseK: InvariantTraverseK[Joined]
}

object JoinKind {
  type Inner[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, B, ?[_]]]
  type Left[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, OptionTK[B, ?[_]], ?[_]]]

  def left[A[_[_]]: InvariantTraverseK, B[_[_]]: InvariantTraverseK]: Left[A, B] =
    make[A, B, Left[A, B]#Out]("left join")((a, b) => Tuple2KK(a, OptionTK.liftK(b)(Reference.liftOptionK)))

  def inner[A[_[_]]: InvariantTraverseK, B[_[_]]: InvariantTraverseK]: Inner[A, B] =
    make[A, B, Inner[A, B]#Out]("inner join")(Tuple2KK.apply _)

  private def make[A[_[_]], B[_[_]], Joined[_[_]]: InvariantTraverseK](
    name: String
  )(
    build: (A[Reference], B[Reference]) => Joined[Reference]
  ): JoinKind[A, B, Joined] = new JoinKind[A, B, Joined] {
    def buildJoint(a: A[Reference], b: B[Reference]): Joined[Reference] = build(a, b)
    val kind: String = name

    val itraverseK: InvariantTraverseK[Joined] = implicitly[InvariantTraverseK[Joined]]
  }
}
