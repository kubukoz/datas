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
        url("https://kubukoz.com")
      )
    )
  )
)

val compilerPlugins = List(
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
  compilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
  compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  compilerPlugin("com.github.cb372" % "scala-typed-holes" % "0.1.5" cross CrossVersion.full)
)

val commonSettings = Seq(
  scalaVersion := "2.12.10",
  scalacOptions -= "-Xfatal-warnings",
  scalacOptions += "-P:typed-holes:log-level:info",
  name := "datas",
  updateOptions := updateOptions.value.withGigahorse(false),
  libraryDependencies ++= Seq(
    "org.tpolecat" %% "doobie-core" % "0.9.0",
    "org.tpolecat" %% "doobie-postgres" % "0.9.0",
    "org.tpolecat" %% "doobie-hikari" % "0.9.0",
    "org.typelevel" %% "simulacrum" % "1.0.0",
    "org.typelevel" %% "cats-effect" % "2.2.0",
    "org.typelevel" %% "cats-tagless-macros" % "0.11",
    "org.typelevel" %% "cats-mtl-core" % "0.7.1",
    "co.fs2" %% "fs2-core" % "2.4.4"
  ) ++ compilerPlugins
)

val core =
  project.in(file("core")).settings(commonSettings).settings(name += "-core")

val tests = project
  .settings(commonSettings)
  .settings(
    fork := true,
    libraryDependencies ++= Seq(
      "com.kubukoz" %% "flawless-core" % "0.1.0-M11",
      "com.lihaoyi" %% "pprint" % "0.5.5",
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    )
  )
  .dependsOn(core)

val datas =
  project.in(file(".")).settings(commonSettings).settings(skip in publish := true).dependsOn(core).aggregate(core)
