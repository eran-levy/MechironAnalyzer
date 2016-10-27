package org.mechiron.importer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by eran on 20/09/16.
  */
trait Importer {

  def executeImport(sqlContext: SQLContext, filesLocationForProcess: String): Unit
}
