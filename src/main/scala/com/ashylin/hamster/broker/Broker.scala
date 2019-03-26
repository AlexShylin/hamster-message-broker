package com.ashylin.hamster.broker

import java.io._
import java.net.ServerSocket

import resource.managed

class Broker(homeDir: String) {
  val ic = IndexKeeper(homeDir)
  val writer = new DistributedFileWriter(ic)
  val reader = new DistributedFileReader(ic)


  def run(): Unit = {
    managed(new ServerSocket(8007)).acquireAndGet { server =>
      while (true) {
        for {
          connection <- managed(server.accept)
          input <- managed(new BufferedReader(new InputStreamReader(connection.getInputStream)))
          output <- managed(new PrintWriter(new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
          line <- Stream.continually(input.readLine()).takeWhile(_ != null)
        } {
          val action = line.head
          val message = line.tail
          action match {
            case 'w' =>
              response(output, writer.write(message))
            case 'r' =>
              val readData = reader.read(message)
              response(output, readData)
            case 'm' => response(output, ic.maxIndex().toString)
          }
        }
      }
    }
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
