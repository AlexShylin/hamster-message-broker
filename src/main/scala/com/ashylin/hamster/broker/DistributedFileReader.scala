package com.ashylin.hamster.broker

import java.io.{BufferedReader, FileReader}

import com.ashylin.hamster.FileUtils
import resource.managed

class DistributedFileReader(ic: IndexKeeper) {

  def read(message: String): String = {
    readRange(parseReadRange(message))
  }

  private def readSingle(index: Int): String = {
    val fileOpt = ic.getFileNameByRecordIndex(index)
    fileOpt.fold("") { fileName =>
      managed(new BufferedReader(new FileReader(s"${ic.workingDir}/$fileName")))
        .map(file =>
          FileUtils.searchLine(file, l => FileUtils.indexToData(l)._1 == index)
        ).opt.get
    }
  }

  private def readRange(nums: Array[Int]): String = {
    val records = for(num <- nums) yield readSingle(num)
    records.mkString(System.lineSeparator())
  }

  private def parseReadRange(str: String): Array[Int] = {
    val arr = for (range <- str.split(','))
      yield
        if (range contains '-') {
          val nums = range split '-'
          Range(nums(0).toInt, nums(1).toInt+1).toArray
        } else {
          Array(range.toInt)
        }
    arr.flatMap(x => x.map(y => y))
  }
}

/*
object test extends App {
  val ic = IndexKeeper(s"${System.getProperty("user.home")}/HAMSTER_HOME")
  println(new DistributedFileReader(ic).read("11-33,22,78-88,78900"))
}
*/
