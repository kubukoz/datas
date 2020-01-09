package datas

import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import cats.~>
import cats.tagless.implicits._
import datas.tagless.TraverseK

object schemas {
  private val columnToStRef: ColumnK ~> Reference = λ[ColumnK ~> Reference] {
    case ColumnK.Named(name, get) =>
      Reference.Single(ReferenceData.Column(name, none), get)

    case ColumnK.Optional(underlying) =>
      Reference.liftOption(columnToStRef(underlying))
  }

  def column[Type: Get](name: String): ColumnK[Type] =
    ColumnK.Named(Column(name), Get[Type])

  def caseClassSchema[F[_[_]]: TraverseK](name: TableName, columns: F[ColumnK]): QueryBase[F] =
    QueryBase.FromTable(name, columns.mapK(columnToStRef), TraverseK[F])

  final case class TableName(name: String) extends AnyVal {
    private[datas] def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  //todo naming
  sealed trait ColumnK[A] extends Product with Serializable {
    def optional: ColumnK[Option[A]] = ColumnK.Optional(this)
  }

  object ColumnK {
    final case class Named[A](name: Column, get: Get[A]) extends ColumnK[A]
    final case class Optional[A](underlying: ColumnK[A]) extends ColumnK[Option[A]]
  }

  final case class Column(name: String) extends AnyVal {
    def showQuoted: String = "\"" + name + "\""
  }

}
