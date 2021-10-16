package com.apple.Demo

/**
  * @Program: flink-scala
  * @ClassName: BloomDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-15 21:15
  * @Version 1.1.0
  **/
object BloomDemo {
  def main(args: Array[String]): Unit = {
  }
}

// 自定义一个布隆过滤器，主要就是一个位图和hash函数
class Bloom(size: Long) extends Serializable {
  private val cap = size // 默认cap应该是2的整次幂

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    // 返回hash值，要映射到cap范围内
    (cap - 1) & result
  }
}