package com.ashylin

import org.scalatest._

class MySpec extends FlatSpec {
  this: Suite =>

  behavior of "This wonderful system"

  it should "save the world" in {
    val list = Nil
    assert(list.isEmpty)
  }

}
