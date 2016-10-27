name := "MechironAnalyzer"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.6"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.6"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.10"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"


libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.39"
// https://mvnrepository.com/artifact/xerces/xercesImpl
libraryDependencies += "xerces" % "xercesImpl" % "2.9.1"

//exclude org.mortbay.jetty
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.4" excludeAll(ExclusionRule(organization = "javax.servlet"), ExclusionRule(organization = "org.mortbay.jetty"))

// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.2.1" excludeAll ExclusionRule(organization = "javax.servlet")


// https://mvnrepository.com/artifact/com.databricks/spark-csv_2.10
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive_2.10
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.4.1"


