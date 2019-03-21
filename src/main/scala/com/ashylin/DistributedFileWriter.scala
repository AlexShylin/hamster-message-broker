package com.ashylin

import java.io._
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import resource.managed

import scala.collection.JavaConverters._
import scala.collection.concurrent._
import scala.collection.convert.decorateAsScala.mapAsScalaConcurrentMapConverter
import scala.collection.mutable.ListBuffer

object DistributedFileWriter {

  type FileName = String
  case class DataFileContent(numbers: ListBuffer[Int], var full: Boolean)

}

class DistributedFileWriter(workingDir: String, approximateFileSize: Int) {

  import FileUtils._
  import DistributedFileWriter._

  ///****************/
  //* Constructors *//
  /****************///

  initDirIfEmpty(workingDir)
  val indexFilePath = s"$workingDir/$indexFileName"

  val indexFileDescriptor = new FileOutputStream(indexFilePath, true)

  val indexCache: Map[FileName, DataFileContent] = {
    // correct way of creation Scala's concurrent map
    val map = new ConcurrentHashMap[FileName, DataFileContent]().asScala

    val indexFileContent = readAllFileAutoClose(new BufferedReader(new FileReader(indexFilePath)))
    indexFileContent.filter(_.nonEmpty).foreach(line => {
      // add each line to map
      val (index, content) = indexToData(line)
      val dataFileName = numberToDataFileName(index)
      val isFull = content.endsWith("_")
      val numbersString = if (isFull) content.init else content
      val numbers = numbersString.split(',').filter(_.nonEmpty).map(_.toInt).to[ListBuffer]
      map.put(dataFileName, DataFileContent(numbers, isFull))
    })
    map
  }

  def this(workingDir: String) = {
    this(workingDir, 1048576/*one MB in bytes*/ * 32)
  }


  ///******************/
  //* Public writers *//
  /******************///

  def write(newData: String): Unit = {
    val newIndex = indexToData(newData)._1
    validateIndex(newIndex)

    val incompleteFiles: Iterable[FileName] = indexCache.filterNot(_._2.full).keys.map(numberToDataFileName)

    val incompleteFile: FileName =
      if (incompleteFiles.size > 1) throw new IllegalStateException(".index file is corrupted")
      else incompleteFiles.head

    val fileToWriteIn: FileName =
      if (tooBig(incompleteFile)) {
        val newDataFileName: FileName = createNewDataFile()
        cacheAddRecord(incompleteFile, newDataFileName)
        indexFileFinishLine()
        indexFileAddKey(newDataFileName)
        newDataFileName
      } else incompleteFile

    val pathToWriteIn = s"$workingDir/$fileToWriteIn"

    writeToDataFile(pathToWriteIn, newData)
    updateCache(fileToWriteIn, newIndex)
    updateIndexFile(indexFileDescriptor, newIndex)
  }


  ///*****************/
  //* private utils *//
  /*****************///


  private def validateIndex(index: Int): Unit = {
    val indexPresent = indexCache.exists { case (_, value) => value.numbers.contains(index)}
    if (indexPresent) throw new UnsupportedOperationException(s"Record number $index is already indexed")
  }

  private def writeToDataFile(path: String, data: String): Unit = {
    managed(new BufferedWriter(new FileWriter(path, true)))
      .acquireAndGet(file => {
        file.write(data)
        file.newLine()
      })
  }

  private def updateIndexFile(writer: FileOutputStream, index: Int): Unit = {
    writer.write(s"${index.toString},".getBytes)
  }

  private def updateCache(fileName: FileName, newNumber: Int): Unit = {
    indexCache.filter(_._1 == fileName).foreach {
      case (_, value) => value.numbers += newNumber
    }
  }

  private def createNewDataFile(): String = {
    val filesAtDir = getListOfFiles(workingDir)
    val names = filesAtDir.map(_.getName).filter(_.matches("[0-9]{4}"))
    val nextName = names.length + 1
    val newFileName = numberToDataFileName(nextName)
    val path = s"$workingDir/$newFileName"

    Files.createFile(Paths.get(path))
    newFileName
  }

  private def tooBig(fileName: String): Boolean =
    new File(s"$workingDir/$fileName").length() > approximateFileSize

  private def cacheAddRecord(fullFileName: FileName, newFileName: FileName) = {
    // mark old file as full
    indexCache.filter(_._1 == fullFileName).foreach {
      case (_, value) => value.full = true
    }
    indexCache.put(newFileName, DataFileContent(ListBuffer(), full = false))
  }

  private def indexFileFinishLine(): Unit = indexFileDescriptor.write(s"_${System.lineSeparator()}".getBytes())

  private def indexFileAddKey(key: String): Unit = indexFileDescriptor.write(s"$key:".getBytes())
}

object runner extends App {
  val number = 10
  new DistributedFileWriter(s"${System.getProperty("user.home")}/HAMSTER_HOME", 15)
    .write(s"${number.toString}:data")
}

object smoke extends App {
  val oneMB = 1048576
  val start = System.currentTimeMillis()
  val w = new DistributedFileWriter(s"${System.getProperty("user.home")}/HAMSTER_HOME", oneMB)
  for (i <- 11 to 100000) {
    if (i % 1000 == 0) println(i)
    w.write(s"$i:${UUID.randomUUID()}")
  }
  println(s"Took ${System.currentTimeMillis() - start}ms")
}
