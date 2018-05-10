package com.vikas

object Main {
  def main(args: Array[String]): Unit = {
    val a = List("11","21","31","41","51")
    val b = List("1","4","3")

    def reduceFunctionToMax(a: List[String], b: List[String]): List[String] = {
      if (a(1).toLong > b(1).toLong && a(2).toInt >= b(2).toInt) {
        a
      }
      else {
        b
      }
    }
    println(reduceFunctionToMax(a, b))
  }
}


