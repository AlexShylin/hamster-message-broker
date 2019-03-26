package com.ashylin.hamster.client

import java.io._
import java.net.Socket

import com.ashylin.hamster.FileUtils
import resource.managed

import scala.util.Random

object Persist {
  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      throw new IllegalArgumentException("You list of brokers must not be empty. Format is [ip:port] space separated")

    if (!args.forall(_.contains(':')))
      throw new IllegalArgumentException("You must provide list of brokers in [ip:port] format")

    val numBrokers = args.length

    for {
      consoleIn <- managed(new BufferedReader(new InputStreamReader(System.in)))
    } {
      val dataToPersist = FileUtils.readAllStreamOnReader(consoleIn)
      val splitedData = split(dataToPersist, numBrokers)
      val brokerAddresses = args.map(adr => adr.split(':'))
      val brokerToData = brokerAddresses.zip(splitedData)

      brokerToData.foreach { case (a, d) =>
        val host = a(0)
        val port = a(1).toInt
        for {
          connection <- managed(new Socket(host, port))
          out <- managed(new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
          in <- managed(new BufferedReader(new InputStreamReader(connection.getInputStream)))
        } {
          out.println(d.mkString(System.lineSeparator()))
          out.flush()
          while (in.ready()) {
            val answer = in.readLine
            if (answer != "Success")
              System.err.println(s"Error while persisting data to $host:$port")
          }
        }
      }
    }
  }

  private def split(data: Array[String], numPartitions: Int): Array[Array[String]] = {
    val len = data.length
    val defaultChunkSize = len / numPartitions
    val additionalChunkSize = len % numPartitions
    val winnerIndex = doLotteryForAdditionalRecords(numPartitions)
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

  private def doLotteryForAdditionalRecords(upperBound: Int): Int = {
    Random.nextInt(upperBound)
  }
}
