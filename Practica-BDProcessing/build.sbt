ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Big-Data-Processing",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1"
    ),
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-exports",
      "java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )