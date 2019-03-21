package com.ashylin

import java.io._
import java.net.ServerSocket

import resource.managed

class Broker(homeDir: String) {
  val filePath = s"$homeDir/file"
  val indexFilePath = s"$homeDir/.index"


  def run(): Unit = {
    for {
      server <- managed(new ServerSocket(8007))
      connection <- managed(server.accept)
      input <- managed(new BufferedReader(new InputStreamReader(connection.getInputStream)))
      output <- managed(new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
      line <- Stream.continually(input.readLine()).takeWhile(_ != null)
    } {
      val action = line.head
      val message = line.tail
      action match {
        case 'w' => write(message)
        case 'r' => read(parseReadRange(message), output)
      }
    }
  }

  private def write(line: String) = {
    managed(new BufferedWriter(new FileWriter(filePath, true)))
      .acquireAndGet(file => {
        file.write(line)
        file.write(System.lineSeparator)
      })
  }

  private def read(nums: Array[Int], out: PrintWriter) = {
    managed(new BufferedReader(new FileReader(filePath)))
      .acquireAndGet(file =>
        for {
          num <- nums
        } {
          val wantNum = num.toString
          val found = FileUtils.searchLine(file, l => l.split(':')(0) == wantNum)
          response(out, found)
        })
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

  def response(out: PrintWriter, message: String) = {
    out.println(message)
    out.flush()
  }
}

object Broker extends App {
  val homdir = "/Users/alexandershylin/HAMSTER_HOME"

  // starting broker
  val broker = new Broker(homdir)
  broker.run()
}
