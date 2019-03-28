package com.ashylin.hamster.client

import java.io._
import java.net.Socket

import com.ashylin.hamster.FileUtils
import resource.managed

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success}

object Persist {
  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      throw new IllegalArgumentException("List of brokers must not be empty. Format is {[ip:port] [ip:port] ...}")

    if (!args.forall(_.contains(':')))
      throw new IllegalArgumentException("You must provide list of brokers in [ip:port] format")

    val numBrokers = args.length

    val dataToPersist = FileUtils.readAllStream(new BufferedReader(new InputStreamReader(System.in)))
    val splitedData = split(dataToPersist, numBrokers)
    val brokerAddresses = args.map(adr => adr.split(':'))
    val brokerToData = brokerAddresses.zip(splitedData)

    val asyncRequests = brokerToData.map { case (a, d) =>
      val host = a(0)
      val port = a(1).toInt
      val future = Future {
        for {
          connection <- managed(new Socket(host, port))
          out <- managed(new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
        } {
          out.println(d.mkString(System.lineSeparator()))
          out.flush()
        }
        0
      }

      future.onComplete {
        case Success(_: Int) => println("Success")
        case Failure(ex: Exception) => println(s"Operation failed on request to $host:$port with $ex")
      }

      future
    }.toList
    val futureSeq = Future.sequence(asyncRequests)
    Await.ready(futureSeq, Duration.Inf)
  }

  private def split(data: Array[String], numPartitions: Int): Array[Array[String]] = {
    val len = data.length
    val defaultChunkSize = len / numPartitions
    val additionalChunkSize = len % numPartitions
    // do lottery for additional elements
    val winnerIndex = Random.nextInt(numPartitions)
    for {
      i <- (0 until numPartitions).toArray
    } yield {
      val start = {
        if (i > winnerIndex) i * defaultChunkSize + additionalChunkSize
        else i * defaultChunkSize
      }
      val end = {
        if (i >= winnerIndex) (i + 1) * defaultChunkSize + additionalChunkSize
        else (i + 1) * defaultChunkSize
      }
      data.slice(start, end)
    }
  }
}
