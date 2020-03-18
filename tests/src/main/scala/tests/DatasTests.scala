package tests

import java.util.concurrent.Executors

import cats.effect.Blocker
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import doobie.hikari.HikariTransactor
import cats.implicits._
import doobie.Transactor
import scala.concurrent.ExecutionContext
import doobie.util.fragment.Fragment
import doobie.implicits._
import flawless._
import doobie.util.ExecutionContexts

object DatasTests extends IOApp with TestApp {

  val transactor = (Blocker[IO], fixedPool(10))
    .tupled
    .flatMap {
      case (blocker, boundedEc) =>
        HikariTransactor
          .newHikariTransactor[IO](
            "org.postgresql.Driver",
            "jdbc:postgresql://localhost:5432/postgres",
            "postgres",
            "postgres",
            boundedEc,
            blocker
          )
          .evalTap(implicit xa => runMigrations("/init.sql", blocker))
    }
    .widen[Transactor[IO]]

  override def run(args: List[String]): IO[ExitCode] = runTests(args) {
    Suite.resource {
      transactor.map { implicit xa =>
        new BasicJoinQueryTests().runSuite
      }
    }
  }

  private def fixedPool(size: Int) =
    ExecutionContexts.fixedThreadPool[IO](size)

  private def runMigrations(fileName: String, blocker: Blocker)(implicit xa: Transactor[IO]): IO[Unit] = {
    val load = fs2
      .io
      .readInputStream[IO](
        IO(getClass.getResourceAsStream(fileName)),
        4096,
        blocker
      )
      .through(fs2.text.utf8Decode[IO])
      .compile
      .foldMonoid
      .map(Fragment.const(_))
      .flatMap(_.update.run.transact(xa))
      .void

    val unload = sql"""DROP SCHEMA public CASCADE;CREATE SCHEMA public;""".update.run.transact(xa).void

    //ensure unload happens even if load fails
    unload *> load
  }
}
