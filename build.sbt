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
  compilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8"),
  compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)

val commonSettings = Seq(
  scalaVersion := "2.12.8",
  scalacOptions ++= Options.all,
  fork in Test := true,
  name := "datas",
  updateOptions := updateOptions.value.withGigahorse(false), //may fix publishing bug
  libraryDependencies ++= Seq(
    "org.tpolecat" %% "doobie-core" % "0.7.0",
    "org.tpolecat" %% "doobie-postgres" % "0.7.0",
    "org.typelevel" %% "cats-effect" % "1.4.0",
    "org.typelevel" %% "cats-tagless-macros" % "0.5",
    "org.typelevel" %% "cats-mtl-core" % "0.6.0",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  ) ++ compilerPlugins
)

val datas =
  project.in(file("core")).settings(commonSettings).settings(name += "-core").settings(skip in publish := true)
