/*
 * Distributed line count: Assumes a parallel/networked filesystem in this
 * version.
 */

package edu.luc.cs

import java.net._
import java.nio.file._

import blockperf._
import cs.luc.edu.fileutils._
import org.apache.spark._
import squants.time._

import scala.util.Try

object LineCount {

  // This is the Scala way of doing a "struct". This allows us to change what is computed
  // without having to change anything but countLinesInFile()

  case class LineCountData(lineCount: Int, hostname: String, fileName: String, time: ElapsedTime, space: MemoryUsage)

  case class Config(dir: Option[String] = None, ext: Option[String] = None, slices: Int = 48)

  // This function is evaluated in parallel via the RDD

  def countLinesInFile(fileName: String): LineCountData = {
    val path = Paths.get(fileName)
    val hostname = InetAddress.getLocalHost.getHostName
    val (fileTime, fileSpace, lineCount) = performance {
      Try(Files.readAllLines(path).size()) getOrElse (0)
    }
    LineCountData(lineCount, hostname, fileName, fileTime, fileSpace)
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

    val (lsTime, lsSpace, fileList) = performance {
      getFileList(path, extension)
    }

    // create RDD from generated file listing

    val (rddTime, rddSpace, rdd) = performance {
      spark.parallelize(fileList, slices).map {
        fileName => countLinesInFile(fileName)
      }
    }

    // perform distributed line counting and print all information obtained
    // this is mainly for diagnostic purposes

    val (computeTimeDetails, computeSpaceDetails, text) = performance {
      rdd.map { fileInfo => fileInfo.toString + "\n" } reduce (_ + _)
    }

    // perform distributed line counting and sum up all individual times to get
    // an idea of the actual workload of reading all files serially

    val (computeIndividualTime, computeIndividualSpace, sumIndividualTime) = performance {
      rdd map { _.time } reduce (_ + _)
    }

    // This shows how to count the number of unique hosts involved
    // in the computation.

    val pairs = rdd.map(lc => (lc.hostname, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)

    // perform distributed line counting but only project the total line count

    val (computeTime, computeSpace, sumLineCount) = performance {
      rdd map { _.lineCount } reduce (_ + _)
    }

    // TODO: Get this into CSV form or something better for analysis

    println("Nodes Used")
    println(counts.count())
    counts.collect() foreach println

    println("File Line Counts")
    println(text)

    println("Results")
    println(s"fileList.length=${fileList.length}")
    println(s"sumLineCount=$sumLineCount")

    println("Statistics")
    println(s"rddTime=${rddTime.time in Milliseconds}")
    println(s"lsTime=${lsTime.time in Milliseconds}")
    println(s"computeTime=${computeTime.time in Milliseconds}")
    println(s"sumIndividualTime=${sumIndividualTime.time in Milliseconds}")

    println("Quick look at memory on each Spark node")
    println(rddSpace.free)
    spark.stop()
  }
}
