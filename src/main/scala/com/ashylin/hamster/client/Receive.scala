package com.ashylin.hamster.client

import java.io._
import java.net.Socket
import java.nio.file.{Files, Paths}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import com.ashylin.hamster.FileUtils

import scala.concurrent.ExecutionContext.Implicits.global
import resource.managed

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

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

    val maxIndexes = mutable.HashMap[String, Int]()
    val results: mutable.Buffer[ResponseResult] = mutable.ArrayBuffer[ResponseResult]()

    val asyncRequests = brokersToOffsets.map {
      case (addr, start) =>
        val host = addr._1
        val port = addr._2
        val res = for {
          connection <- managed(new Socket(host, port))
          out <- managed(new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
          in <- managed(new BufferedReader(new InputStreamReader(connection.getInputStream)))
        } yield {
          out.println(s"r$start-max")
          out.flush()
          val response = FileUtils.readAllStream(in)
          val parsedResponse = response.map(FileUtils.indexToData)
          parsedResponse.map(r => ResponseResult(r._1, r._2))
        }
        val future = res.toFuture

        future.onComplete {
          case Success(a: Array[ResponseResult]) =>
            mutex.lock()
            results ++= a
            maxIndexes.put(s"$host:$port", a.max.index)
            mutex.unlock()
          case Failure(ex: Exception) =>
            results ++= Array(
              ResponseResult(Integer.MAX_VALUE, s"Operation failed on request to $host:$port with $ex")
            )
        }

        future
    }.toList

    val futureSeq = Future.sequence(asyncRequests)
    Await.ready(futureSeq, Duration.Inf)

    // writing max indexes to files
    brokers.foreach{b =>
      managed(new PrintWriter(new FileWriter(homdir + "/" + toFileName(b), false))).acquireAndGet{pw =>
        pw.println(maxIndexes(b).toString)
      }
    }

    val finalString = results.sorted.map(_.message).mkString(System.lineSeparator())
    println(finalString)
  }

  def toFileName(address: String): String = address.replace(':', '$')
}
