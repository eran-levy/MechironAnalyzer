package org.mechiron.utils

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/**
  * Connect to Hadoop FS
  * @author Eran Levy
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
