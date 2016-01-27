/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

package edu.luc.cs

import java.io.File
import java.nio.file._
import scala.math.random
import scala.util.Try
import org.apache.spark._

/** Computes an approximation to pi */
object LineCount {

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  // TODO: Eventually, convert to Java 7/8 directory stream
  def getFileList(path : String, ext : String) : Array[String] = {
    require { ext.startsWith(".") }
    val fullPath = new File(path).getAbsolutePath()
    recursiveListFiles( new File(fullPath) ).filter( f => f.getName().endsWith(ext)).map(_.getAbsolutePath())
  }

  def countLinesInFile(fileName : String) : Int = {
    val path = Paths.get(fileName)
    Try(Files.readAllLines(path).size()) getOrElse(0)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LineCount File I/O")
    val spark = new SparkContext(conf)

    // NOTE: This program only needs a directory of files to process recursively.
    // TODO: Use Command-line parsing

    val path = if (args.length > 0) args(0) else "./data"
    val extension = if (args.length > 1) args(1) else ".txt"

    val slices = if (args.length > 2) args(2).toInt else 48

    val count = spark.parallelize(getFileList(path, extension), slices).map {
      fileName => countLinesInFile(fileName)
    }.reduce(_ + _)
    getFileList(path, ".txt") foreach println
    println(s"Line count across all files $count")
    spark.stop()
  }
}
// scalastyle:on println
