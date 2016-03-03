
package cs.luc.edu

import java.io._

package object fileutils {

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getFileList(path: String, ext: String): Array[String] = {
    require { ext.startsWith(".") }
    val fullPath = new File(path).getAbsolutePath()
    recursiveListFiles(new File(fullPath)).filter(f => f.getName().endsWith(ext)).map(_.getAbsolutePath())
  }

}