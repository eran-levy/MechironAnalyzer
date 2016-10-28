package org.mechiron.enhancer

import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.mechiron.utils.MySQLBridge

/**
  * Items Enhancer unit - each retail name its item names/manufacture names/manufacture country names/etc different.
  * Its main role is to unify the names (currently) simply based on the most frequent word.
  * Run the same function for different columns in the "dwdata" fact table located in our Hive server.
  * We run a window function sql query that ranks the most frequent word for a given column.
  *
  * The Hive server has been configured to utilize Spark as its execution engine so quering Hive using the Hive JDBC
  * driver was problematic since it was bit heavy in the Kryo serialization/deseralization processes.
  * Spark handled it quite efficiently, so we initiate an HiveContext to query and then perform the batch updates into
  * the Mysql server.
  *
  *
  * @author Eran Levy
  */
class ItemsEnhancer(sc: SparkContext) {
  val logger = LogManager.getLogger(getClass)

  def sparkFuncEnhance(columnName: String): Unit = {
    logger.debug("starting values enhancer... for tableName: " +columnName)
    val mysqlConnection = new MySQLBridge().connectJdbc()
    //auto commit to false so we will be able to perform btach updates efficiently
    mysqlConnection.setAutoCommit(false)

    val hiveContext = new HiveContext(sc)
    //window function to rank the most frequent word for the given column
    //TODO: replace this window function sql query with the Spark API
    val df = hiveContext.sql("select item_code, " + columnName + " from (select item_code," + columnName + ", rank() over " +
      "(partition by item_code order by thenum desc) as therank from (select item_code, " + columnName + ", count(*) as thenum " +
      "from dwdata group by " + columnName + ", item_code) as b) as y where y.therank=1")

    val ps = mysqlConnection.prepareStatement("UPDATE items SET "+ columnName +"=? WHERE item_code=?")
    var index = 0
    val rowsArr = df.collect()
    logger.debug("successfully collected results from dwdata Hive store...")
    for(row <- rowsArr) {
      val itemCode = row.getString(0)
      val columnToEnhance = row.getString(1)
      ps.setString(1,columnToEnhance)
      ps.setString(2,itemCode)
      ps.addBatch()
      index+=1
      //execute 1000
      if(index%1000 == 0) {
        logger.debug("writing batch to mysql")
        ps.executeBatch()
        logger.debug("successfully wrote batch to mysql")
        index=0
      }
    }
    //execute we still have some left to execute and commit
    if(index>0) {
      logger.debug("we still got updates to perform - writing batch to mysql")
      ps.executeBatch()
      logger.debug("successfully wrote batch to mysql")
    }
    mysqlConnection.commit()
    logger.debug("done! committed batch results to mysql")
  }

}
