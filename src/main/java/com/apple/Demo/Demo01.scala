package com.apple.Demo

import scala.util.Random

/**
  * @Program: flink-scala
  * @ClassName: Demo01
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-08 22:42
  * @Version 1.1.0
  **/
object Demo01 {
  def main(args: Array[String]): Unit = {
    val rand = new Random()
    println(":: " + rand.nextDouble())
  }
}
