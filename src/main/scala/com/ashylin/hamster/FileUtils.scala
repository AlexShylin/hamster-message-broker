package com.ashylin.hamster

import java.io.{BufferedReader, File}
import java.nio.file.{Files, Paths}

import resource.managed

import scala.util.{Failure, Success, Try}

object FileUtils {

  type FileName = String

  val indexFileName = ".index"

  def searchLineAutoClose(br: BufferedReader, cond: String => Boolean): String =
    searchLine(br, cond, true)

  def searchLine(br: BufferedReader, cond: String => Boolean, autoClose: Boolean = false): String = {
    while (br.ready()) {
      val line = br.readLine
      if (cond(line)) return line
    }
    null
  }

  def readAllFileAutoClose(br: BufferedReader): Array[String] = readAllStream(br, autoClose = true)


  def readAllStream(br: BufferedReader, autoClose: Boolean = false): Array[String] = {
    def readAllStreamOnReader(brInner: BufferedReader): Array[String] =
      Stream.continually(brInner.readLine()).takeWhile(_ != null).toArray

    if (autoClose) {
      val tried: Try[Array[String]] = managed(br).map(readAllStreamOnReader).tried
      tried match {
        case Success(_) => tried.get
        case Failure(exception) => throw exception
      }
    }
    else readAllStreamOnReader(br)
  }


  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(f => f.isFile).toList
    } else {
      List[File]()
    }
  }

  def numberToDataFileName(n: Int): String = numberToDataFileName(n.toString)

  def numberToDataFileName(s: String): String = if (s.length < 4) numberToDataFileName("0" + s) else s

  def indexToData(line: String): (Int, String) = {
    val split = line.split(":")
    if (split.isEmpty)
      throw new IllegalArgumentException(s"Message can't be empty, must contain 'index:text', actual $line")
    val index = split(0).toInt
    val data = split.tail.mkString(":")
    (index, data)
  }

  def initDirIfEmpty(path: String): Unit = {
    val indexFilePath = s"$path/$indexFileName"

    val firstDataFileName = "0001"
    val dataFilePath = s"$path/$firstDataFileName"

    val distributedData: List[File] = getListOfFiles(path)

    if (distributedData.isEmpty) {
      // init working directory if empty
      Files.createFile(Paths.get(indexFilePath))
      Files.write(Paths.get(indexFilePath), s"$firstDataFileName:".getBytes())

      Files.createFile(Paths.get(dataFilePath))
    }
  }
}
