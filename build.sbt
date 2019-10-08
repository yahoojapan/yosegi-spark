/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
name := "Yosegi Spark v2.2.0"
version := "1.0"
scalaVersion := "2.11.8"
fork := true
organization := "jp.co.yahoo.yosegi"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-encoding", "UTF-8")

libraryDependencies += "junit" % "junit" % "4.12" % "test"
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test exclude("junit", "junit-dep") 

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

libraryDependencies += "jp.co.yahoo.yosegi" % "yosegi" % "1.0.0"


// release for Maven
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }


description := "Yosegi package."
licenses := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/yahoojapan/yosegi-spark"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/yahoojapan/yosegi-spark"),
    "scm:git@github.com:yahoojapan/yosegi-spark.git"
  )
)

developers := List(
  Developer(
    id    = "koijima",
    name  = "Koji Ijima",
    email = "kijima@yahoo-corp.jp",
    url   = url("http://")
  )
)

publishMavenStyle := true
publishArtifact in Test := false
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

