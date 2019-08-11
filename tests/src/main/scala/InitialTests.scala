import java.util.concurrent.Executors

import cats.data.NonEmptyList
import cats.effect.Blocker
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import doobie.hikari.HikariTransactor
import flawless._
import cats.implicits._
import doobie.Transactor
import scala.concurrent.ExecutionContext
import doobie.util.fragment.Fragment
import doobie.implicits._

object InitialTests extends IOApp {

  val transactor = (Blocker[IO], fixedPool(10).map(ExecutionContext.fromExecutorService))
    .tupled
    .flatMap {
      case (implicit0(blocker: Blocker), boundedEc) =>
        HikariTransactor
          .newHikariTransactor[IO](
            "org.postgresql.Driver",
            "jdbc:postgresql://localhost:5432/postgres",
            "postgres",
            "postgres",
            boundedEc,
            blocker.blockingContext
          )
          .evalTap(runMigrations("./init.sql"))
    }
    .widen[Transactor[IO]]

  override def run(args: List[String]): IO[ExitCode] =
    transactor.use { xa =>
      val tests =
        BasicJoinQueryTests.runSuite(xa)

      runTests(args)(Tests.sequence(NonEmptyList.one(tests)))
    }

  private def fixedPool(size: Int) =
    Resource.make(IO(Executors.newFixedThreadPool(size)))(ec => IO(ec.shutdown()))

  private def runMigrations(fileName: String)(xa: Transactor[IO])(implicit blocker: Blocker): IO[Unit] = {
    val load = fs2
      .io
      .readInputStream[IO](IO(getClass.getClassLoader.getResourceAsStream(fileName)), 4096, blocker.blockingContext)
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
