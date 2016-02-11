/*
 * Distributed line count: Assumes a parallel/networked filesystem in this
 * version.
 */

package edu.luc.cs

import java.io._
import java.net._
import java.nio.file._
import scala.util.Try
import org.apache.spark._

object LineCount {

  case class LineCountData(lineCount: Int, hostname: String, fileName: String, time: Time)

  case class Time(t: Double) {
    val nanoseconds = t.toLong
    val milliseconds = (t / 1.0e6).toLong

    def +(another: Time): Time = Time(t + another.t)

    override def toString(): String = f"Time(t=$t%.2f, ns=$nanoseconds%d, ms=$milliseconds%d)";
  }

  // time a block of Scala code - useful for timing everything!
  // return a Time object so we can obtain the time in desired units

  def nanoTime[R](block: => R): (Time, R) = {
    val t0 = System.nanoTime()
    // This executes the block and captures its result
    // call-by-name (reminiscent of Algol 68)
    val result = block
    val t1 = System.nanoTime()
    val deltaT = t1 - t0
    (Time(deltaT), result)
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getFileList(path: String, ext: String): Array[String] = {
    require { ext.startsWith(".") }
    val fullPath = new File(path).getAbsolutePath()
    recursiveListFiles(new File(fullPath)).filter(f => f.getName().endsWith(ext)).map(_.getAbsolutePath())
  }

  def countLinesInFile(fileName: String): LineCountData = {
    val path = Paths.get(fileName)
    val hostname = InetAddress.getLocalHost.getHostName
    val (fileTime, lineCount) = nanoTime {
      Try(Files.readAllLines(path).size()) getOrElse (0)
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

    // create RDD from generated file listing

    val (rddTime, rdd) = nanoTime {
      spark.parallelize(fileList, slices).map {
        fileName => countLinesInFile(fileName)
      }
    }

    // perform distributed line counting and print all information obtained
    // this is mainly for diagnostic purposes

    val (computeTimeDetails, text) = nanoTime {
      rdd.map { fileInfo => fileInfo.toString + "\n" } reduce (_ + _)
    }

    // perform distributed line counting and sum up all individual times to get
    // an idea of the actual workload of reading all files serially

    val (computeIndividualTime, sumIndividualTime) = nanoTime {
      rdd map { _.time } reduce (_ + _)
    }

    // perform distributed line counting but only project the total line count

    val (computeTime, sumLineCount) = nanoTime {
      rdd map { _.lineCount } reduce (_ + _)
    }

    // TODO: Get this into CSV form or something better for analysis

    println("File Line Counts")
    println(text)

    println("Results")
    println(s"fileList.length=${fileList.length}")
    println(s"sumLineCount=$sumLineCount")

    println("Statistics")
    println(s"rddTime=${rddTime.milliseconds} ms")
    println(s"lsTime=${lsTime.milliseconds} ms")
    println(s"computeTime=${computeTime.milliseconds} ms")
    println(s"sumIndividualTime=${sumIndividualTime.milliseconds} ms")
    spark.stop()
  }
}
