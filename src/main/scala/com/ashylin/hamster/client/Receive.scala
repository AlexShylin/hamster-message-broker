package com.ashylin.hamster.client

import java.io._
import java.net.Socket
import java.nio.file.{Files, Paths}
import java.util.concurrent.locks.ReentrantLock

import com.ashylin.hamster.FileUtils
import resource.managed

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Receive {

  val mutex = new ReentrantLock

  case class ResponseResult(index: Int, message: String) extends Ordered[ResponseResult] {
    override def compare(that: ResponseResult): Int = index.compareTo(that.index)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      throw new IllegalArgumentException("Homedir and list of brokers must be provided. " +
        "Format is {/path/to/home/dir [ip:port] [ip:port] ...}")

    val homdir = args(0)
    val brokers = args.tail

    if (!brokers.forall(_.contains(':')))
      throw new IllegalArgumentException("You must provide list of brokers in [ip:port] format space separated")

    val lastReadOffsets = brokers.map { b =>
      val path = homdir + "/" + toFileName(b)
      val file = new File(path)
      if (!file.exists) {
        Files.createFile(Paths.get(path))
        Files.write(Paths.get(path), "0".getBytes())
      }
      FileUtils.readAllStream(new BufferedReader(new FileReader(file))).head
    }

    val brokersParsed = brokers
      .map(s => s.splitAt(s.indexOf(':')))
      .map { case (host, portStr) => (host, portStr.tail.toInt) }

    val brokersToOffsets = brokersParsed.zip(lastReadOffsets)

    val asyncRequests = brokersToOffsets.map {
      case (addr, start) =>
        val host = addr._1
        val port = addr._2
        Future {
          val connection = new Socket(host, port)
          var result: ArrayBuffer[ResponseResult] = null

          // request
          val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream)))
          out.println(s"r${start.toInt + 1}-max")
          out.flush()

          // response
          val in = new BufferedReader(new InputStreamReader(connection.getInputStream))
          val response = new ArrayBuffer[String]()
          var line: String = null
          while ( {
            line = in.readLine
            line != null
          }) {
            response += line
          }

          // parse response
          val parsedResponse = response.map(FileUtils.indexToData)
          result = parsedResponse.map(r => ResponseResult(r._1, r._2))

          // close resources
          out.close()
          in.close()
          connection.close()

          // return result
          finish(homdir + "/" + s"$host$$$port", result)
        }

    }.toList

    val futureSeq = Future.sequence(asyncRequests)
    Await.result(futureSeq, Duration.Inf)


  }

  def finish(filePath: String, arr: ArrayBuffer[ResponseResult]) = {
    mutex.lock()
    if (arr.filter(e => e.index > -1).nonEmpty) {
      managed(new PrintWriter(new FileWriter(filePath, false))).acquireAndGet { pw =>
        pw.println(arr.max.index.toString)
      }
    }
    println(arr.map(_.message).mkString(System.lineSeparator()))
    mutex.unlock()
  }

  def toFileName(address: String): String = address.replace(':', '$')
}
