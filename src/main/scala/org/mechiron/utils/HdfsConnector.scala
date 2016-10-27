package org.mechiron.utils

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/**
  * Created by eran on 10/09/16.
  */
class HdfsConnector {

  def connect(): FileSystem = {
    val hadoopHome = ConfigFactory.getProperty("hadoop_home")
    val conf = new Configuration()
    conf.addResource(new Path(hadoopHome.get+"/etc/hadoop/core-site.xml"))
    conf.addResource(new Path(hadoopHome.get+"/etc/hadoop/hdfs-site.xml"))
    FileSystem.get(conf)
  }

}
