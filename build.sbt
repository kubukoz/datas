inThisBuild(
  List(
    organization := "com.kubukoz",
    homepage := Some(url("https://github.com/kubukoz/datas")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
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
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.1").cross(CrossVersion.full),
  compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),
  compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)

val commonSettings = Seq(
  scalaVersion := "2.12.10",
  scalacOptions ++= Options.all,
  name := "datas",
  updateOptions := updateOptions.value.withGigahorse(false), //may fix publishing bug
  libraryDependencies ++= Seq(
    "org.tpolecat" %% "doobie-core" % "0.8.2",
    "org.tpolecat" %% "doobie-postgres" % "0.8.2",
    "org.tpolecat" %% "doobie-hikari" % "0.8.2",
    "org.typelevel" %% "cats-effect" % "2.0.0",
    "org.typelevel" %% "cats-tagless-macros" % "0.10",
    "org.typelevel" %% "cats-mtl-core" % "0.7.0"
  ) ++ compilerPlugins
)

val core =
  project.in(file("core")).settings(commonSettings).settings(name += "-core")

val tests = project
  .settings(commonSettings)
  .settings(
    fork := true,
    libraryDependencies ++= Seq(
      "com.kubukoz" %% "flawless-core" % "0.1.0-M2-2",
      "com.lihaoyi" %% "pprint" % "0.5.5"
    )
  )
  .dependsOn(core)

val datas = project.in(file(".")).settings(commonSettings).settings(skip in publish := true).dependsOn(core).aggregate(core)
