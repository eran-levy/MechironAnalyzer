package org.mechiron.tests

import org.mechiron.utils.ConfigFactory
import org.scalatest.FunSuite

/**
  * Created by eran on 16/09/16.
  */
class Test extends FunSuite{


 test("test get config property") {
   assert(ConfigFactory.getProperty("mechiron_hdfs_data_folder")!=None)
 }

  test("test get config property is None for unavailable config property") {
    assert(ConfigFactory.getProperty("noconfig")==None)
  }
}
