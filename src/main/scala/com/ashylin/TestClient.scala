package com.ashylin

import java.net.{ServerSocket, Socket}
import java.io._
import resource._

object TestClient {
  def main(args: Array[String]): Unit = {
    for {connection <- managed(new Socket("localhost", 8007))
         outStream <- managed(connection.getOutputStream)
         val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outStream)))
         inStream <- managed(new InputStreamReader(connection.getInputStream))
         val in = new BufferedReader(inStream)
    } {
      out.println("w100004:kek")
      out.println("w100005:kek:kek")
      out.println("w100006:kek,kek,kek")
      out.println("r100001-100003")
      out.flush()
      while (true) println(in.readLine)
    }
  }
}