inThisBuild(
  List(
    organization := "com.kubukoz",
    homepage := Some(url("https://github.com/kubukoz/datas")),
    licenses := List(
      "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
    ),
    developers := List(
      Developer(
        "kubukoz",
        "Jakub Kozłowski",
        "kubukoz@gmail.com",
        url("https://kubukoz.com"),
      )
    ),
  )
)

val compilerPlugins = List(
  compilerPlugin("org.typelevel" %% "kind-projector" % "0.13.3" cross CrossVersion.full),
  compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
)

val commonSettings = Seq(
  scalaVersion := "2.13.16",
  scalacOptions -= "-Xfatal-warnings",
  name := "datas",
  libraryDependencies ++= Seq(
    "org.tpolecat" %% "doobie-core" % "1.0.0-M4",
    "org.tpolecat" %% "doobie-postgres" % "1.0.0-M4",
    "org.tpolecat" %% "doobie-hikari" % "1.0.0-M4",
    "org.typelevel" %% "cats-effect" % "3.2.9",
    "org.typelevel" %% "cats-tagless-core" % "0.14.0",
    "org.typelevel" %% "cats-mtl" % "1.2.1",
    "co.fs2" %% "fs2-core" % "3.0.4",
  ) ++ compilerPlugins,
)

val core =
  project.in(file("core")).settings(commonSettings).settings(name += "-core")

val tests = project
  .settings(commonSettings)
  .settings(
    fork := true,
    libraryDependencies ++= Seq(
      "com.kubukoz" %% "flawless-core" % "0.1.0-M12+38-23b5ce1a-SNAPSHOT",
      "com.lihaoyi" %% "pprint" % "0.5.5",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
    ),
  )
  .dependsOn(core)

val datas =
  project.in(file(".")).settings(commonSettings).settings((publish / skip) := true).dependsOn(core).aggregate(core)
