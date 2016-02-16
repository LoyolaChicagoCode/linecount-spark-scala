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
import cs.luc.edu.timing._
import cs.luc.edu.fileutils._

object LineCount {

  // This is the Scala way of doing a "struct". This allows us to change what is computed 
  // without having to change anything but countLinesInFile()

  case class LineCountData(lineCount: Int, hostname: String, fileName: String, time: Time)

  case class Config(dir: Option[String] = None, ext: Option[String] = None,
    slices: Int = 48)

  // This function is evaluated in parallel via the RDD

  def countLinesInFile(fileName: String): LineCountData = {
    val path = Paths.get(fileName)
    val hostname = InetAddress.getLocalHost.getHostName
    val (fileTime, lineCount) = nanoTime {
      Try(Files.readAllLines(path).size()) getOrElse (0)
    }
    LineCountData(lineCount, hostname, fileName, fileTime)
  }

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("LineCount", "1.0")
      opt[String]('d', "dir") action { (x, c) =>
        c.copy(dir = Some(x))
      } text ("dir is a String property")
      opt[String]('e', "extension") action { (x, c) =>
        c.copy(ext = Some(x))
      } text ("ext is a String property")
      opt[Int]('s', "slices") action { (x, c) =>
        c.copy(slices = x)
      } text ("slices is an Int property")
      help("help") text ("prints this usage text")

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config())
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LineCount File I/O")
    val spark = new SparkContext(conf)
    val appConfig = parseCommandLine(args).getOrElse(Config())
    val path = appConfig.dir.getOrElse("./data")
    val extension = appConfig.ext.getOrElse(".txt")
    val slices = appConfig.slices

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
