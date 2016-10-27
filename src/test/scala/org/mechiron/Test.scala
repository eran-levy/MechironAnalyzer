package org.mechiron

import java.io.File

import org.mechiron.utils.ConfigFactory

/**
  * Created by eran on 16/09/16.
  */
object Test {
  val folderName = ConfigFactory.getProperty("mechiron_downloaded_data_location")
  def main(args: Array[String]) {
    //    try {
    //
    //    }catch {
    //      case ex:ClassNotFoundException => println(ex.printStackTrace())
    //    }
    print(folderName.get)
//    val filesFolder = new File(folderName)
//    val directoriesToIterate = filesFolder.listFiles().filter(x=>x.isDirectory)
//    for(somef <- directoriesToIterate) {
//      print(somef)
//    }
  }

}
