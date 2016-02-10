/*
 * Distributed line count: Assumes a parallel/networked filesystem in this
 * version.
 */

package edu.luc.cs

import java.io._
import java.net._
import java.nio.file._
import scala.math.random
import scala.util.Try
import org.apache.spark._

/** Computes an approximation to pi */
object LineCount {

  // case class is the Scala equivalent of a struct (but even more awesome)
  case class Time(t : Double) {
     val ns = t.toLong
     val ms = (t * 1.0e6).toLong

     def +(another : Time) : Time = Time(t + another.t)
  }

  // time a block of Scala code - useful for timing everything!
  def nanoTime[R](block: => R): (Time, R) = {
    val t0 = System.nanoTime()
    val result = block // block is eval'd call-by-name (Algol 68, anyone?)
    val t1 = System.nanoTime()
    val deltaT = t1 - t0
    ( Time(deltaT), result)
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getFileList(path : String, ext : String) : Array[String] = {
    require { ext.startsWith(".") }
    val fullPath = new File(path).getAbsolutePath()
    recursiveListFiles( new File(fullPath) ).filter( f => f.getName().endsWith(ext)).map(_.getAbsolutePath())
  }

  case class LineCountData(lineCount : Int, hostname : String, fileName : String, t : Time)

  def countLinesInFile(fileName : String) : LineCountData = {
    val path = Paths.get(fileName)
    val hostname = InetAddress.getLocalHost.getHostName
    val (fileTime, lineCount) = nanoTime {
       Try(Files.readAllLines(path).size()) getOrElse(0)
    }
    LineCountData(lineCount, hostname, fileName, fileTime)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LineCount File I/O")
    val spark = new SparkContext(conf)
    val path = if (args.length > 0) args(0) else "./data"
    val extension = if (args.length > 1) args(1) else ".txt"

    val slices = if (args.length > 2) args(2).toInt else 48

    val (lsTime, fileList) = nanoTime {
      getFileList(path, extension)
    }

    // This can be commented out if you don't want detailed performance
    // data per file (e.g. where it was computed, line count, delta time)

 
    val (rddTime, rdd) = nanoTime {
    	spark.parallelize(fileList, slices).map {
          fileName => countLinesInFile(fileName)
        }
    }

    val (computeTimeDetails, text) = nanoTime {
      rdd.map { fileInfo => fileInfo.toString } reduce(_ + _)
    }

    // Let's find out how long it takes (sequentially) to read all files
    // factoring out parallelism.

    val (computeIndividualTime, sumIndividualTime) = nanoTime {
      rdd map { _.t } reduce(_ + _)
    }

    // This does the actual line count for all files in the fileset

    val (computeTime, sumLineCount) = nanoTime {
      rdd map { _.lineCount } reduce(_ + _)
    }

    println("File Line Counts")
    println(text)

    println("Statistics")
    println(s"#files=${fileList.length}, rddTime=$rddTime.ms, lsTime=$lsTime.ms, computeTime=$computeTime.ms, sumIndividualTime=$sumIndividualTime.ms")
    println(s"line count=$sumLineCount")
    spark.stop()
  }
}
