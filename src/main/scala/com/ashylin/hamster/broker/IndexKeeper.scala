package com.ashylin.hamster.broker

import java.io.{BufferedReader, FileOutputStream, FileReader}
import java.util.concurrent.ConcurrentHashMap

import com.ashylin.hamster.FileUtils._

import scala.collection.concurrent.Map
import scala.collection.convert.decorateAsScala.mapAsScalaConcurrentMapConverter
import scala.collection.mutable.ListBuffer

case class IndexKeeper(workingDir: String) {

  case class DataFileContent(numbers: ListBuffer[Int], var full: Boolean)

  ////***************/
  ///* Constructor *//
  //***************///

  initDirIfEmpty(workingDir)

  val indexFilePath = s"$workingDir/$indexFileName"

  val indexFileOutputDescriptor = new FileOutputStream(indexFilePath, true)

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


  ////********************/
  ///* Public functions *//
  //********************///

  def maxIndex(): Int = maxIndex(getIncompleteFile)

  def maxIndex(fileName: FileName): Int = indexCache(fileName).numbers.last

  def getDataForNewRecord: (Int, FileName) = {
    val file = getIncompleteFile
    val index = maxIndex(file) + 1
    (index, file)
  }

  def indexNewFile(incomplete: FileName, empty: FileName): Unit = {
    addRecordToCache(incomplete, empty)
    finishLineAtIndexFile()
    declareNewKeyAtIndexFile(empty)
  }

  def updateIndex(key: FileName, valueToAdd: Int): Unit = {
    updateCache(key, valueToAdd)
    appendNumberToIndexFile(valueToAdd)
  }

  def getFileNameByRecordIndex(index: Int): Option[FileName] = {
    val filteredCache = indexCache.filter(_._2.numbers.contains(index))
    if (filteredCache.size > 1) {
      throw new IllegalStateException(
        "One index was met multiple times. " +
        "This must not be happen." +
        s"Was met at files: ${filteredCache.keys.mkString(",")}"
      )
    } else if (filteredCache.isEmpty) {
      None
    } else Some(filteredCache.keys.head)
  }


  ////******************/
  ///* File functions *//
  //******************///

  private def finishLineAtIndexFile(): Unit =
    indexFileOutputDescriptor.write(s"_${System.lineSeparator()}".getBytes())

  private def declareNewKeyAtIndexFile(key: FileName): Unit =
    indexFileOutputDescriptor.write(s"$key:".getBytes())

  private def appendNumberToIndexFile(index: Int): Unit = {
    indexFileOutputDescriptor.write(s"${index.toString},".getBytes)
  }


  ////*******************/
  ///* Cache functions *//
  //*******************///

  def getIncompleteFile: FileName = {
    val incompleteFiles: Iterable[FileName] = indexCache.filterNot(_._2.full).keys.map(numberToDataFileName)

    val incompleteFile: FileName =
      if (incompleteFiles.size > 1) throw new IllegalStateException(".index file is corrupted")
      else incompleteFiles.head

    incompleteFile
  }

  private def addRecordToCache(fullFileName: FileName, newFileName: FileName) = {
    // mark old file as full
    indexCache.filter(_._1 == fullFileName).foreach {
      case (_, value) => value.full = true
    }
    indexCache.put(newFileName, DataFileContent(ListBuffer(), full = false))
  }

  private def updateCache(fileName: FileName, newNumber: Int): Unit = {
    indexCache.filter(_._1 == fileName).foreach {
      case (_, value) => value.numbers += newNumber
    }
  }
}
