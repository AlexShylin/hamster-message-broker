package com.ashylin.hamster.broker

import java.io._
import java.net.ServerSocket

import resource.managed

class Broker(homeDir: String, port: String) {
  val ic = IndexKeeper(homeDir)
  val writer = new DistributedFileWriter(ic)
  val reader = new DistributedFileReader(ic)


  def run(): Unit = {
    managed(new ServerSocket(port.toInt)).acquireAndGet { server =>
      println(s"Started server on port $port")
      while (true) {
        try {
          for {
            connection <- managed(server.accept)
            input <- managed(new BufferedReader(new InputStreamReader(connection.getInputStream)))
            output <- managed(new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
          } {
            try {
              do {
                val request = input.readLine()
                doBrokerJob(output, request)
              } while (input.ready())
            } catch {
              case e: Exception => e.printStackTrace(output)
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  private def doBrokerJob(output: PrintWriter, line: String): Unit = {
    val action = line.head
    val message = line.tail
    action match {
      case 'w' =>
        response(output, writer.write(message))
      case 'r' =>
        val readData = reader.read(message)
        response(output, readData)
      case 'm' =>
        response(output, ic.maxIndex().toString)
    }
  }

  def response(out: PrintWriter, message: String): Unit = {
    out.println(message)
    out.flush()
  }
}

object BrokerRunner extends App {
  val broker = new Broker(args(0), args(1))
  broker.run()
}
