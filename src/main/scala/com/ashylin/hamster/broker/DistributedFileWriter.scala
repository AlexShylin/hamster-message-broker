package com.ashylin.hamster.broker

import java.io._
import java.nio.file.{Files, Paths}

import com.ashylin.hamster.FileUtils._
import resource.managed

class DistributedFileWriter(ic: IndexKeeper, approximateFileSize: Int) {

  ////****************/
  ///* Constructors *//
  //****************///

  def this(ic: IndexKeeper) = {
    this(ic, 1048576 /*one MB in bytes*/ * 32)
  }


  ////******************/
  ///* Public writers *//
  //******************///

  def write(newData: String): String = {
    val (newIndex, incompleteFile) = ic.getDataForNewRecord

    val fileToWriteIn: FileName =
      if (fileTooBig(incompleteFile)) {
        val newDataFileName: FileName = createNewDataFile()
        ic.indexNewFile(incompleteFile, newDataFileName)
        newDataFileName
      } else incompleteFile

    val pathToWriteIn = s"${ic.workingDir}/$fileToWriteIn"

    writeToDataFile(pathToWriteIn, newData)
    ic.updateIndex(fileToWriteIn, newIndex)
    newIndex.toString
  }


  ////*****************/
  ///* private utils *//
  //*****************///

  private def writeToDataFile(path: String, data: String): Unit = {
    managed(new BufferedWriter(new FileWriter(path, true)))
      .acquireAndGet(file => {
        file.write(data)
        file.newLine()
      })
  }

  private def createNewDataFile(): String = {
    val filesAtDir = getListOfFiles(ic.workingDir)
    val names = filesAtDir.map(_.getName).filter(_.matches("[0-9]{4}"))
    val nextName = names.length + 1
    val newFileName = numberToDataFileName(nextName)
    val path = s"${ic.workingDir}/$newFileName"

    Files.createFile(Paths.get(path))
    newFileName
  }

  private def fileTooBig(fileName: String): Boolean =
    new File(s"${ic.workingDir}/$fileName").length() > approximateFileSize

}

/*
object runner extends App {
  val ic = IndexKeeper(s"${System.getProperty("user.home")}/HAMSTER_HOME")
  new DistributedFileWriter(ic, 15)
    .write("data")
}

object smoke extends App {
  val oneMB = 1048576
  val start = System.currentTimeMillis()
  val ic = IndexKeeper(s"${System.getProperty("user.home")}/HAMSTER_HOME")
  val w = new DistributedFileWriter(ic, oneMB)
  for (i <- 1 to 100000) {
    if (i % 1000 == 0) println(i)
    w.write(UUID.randomUUID().toString)
  }
  println(s"Took ${System.currentTimeMillis() - start}ms")
}
*/
