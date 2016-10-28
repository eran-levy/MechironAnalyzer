package org.mechiron.importer

import org.apache.spark.sql.SQLContext

/**
  * Interface for an ETL importer
  *
  * @author Eran Levy
  */
trait Importer {
  def executeImport(sqlContext: SQLContext, filesLocationForProcess: String): Unit
}
