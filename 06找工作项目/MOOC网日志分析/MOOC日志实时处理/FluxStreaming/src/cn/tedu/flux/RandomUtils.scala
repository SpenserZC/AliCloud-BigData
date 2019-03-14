package cn.tedu.flux

import scala.util.Random

object RandomUtils {
  private val rand = new Random()
  def getRandInt(len:Int)={
    var str = ""
    for(n <- 0 to len){
      str += rand.nextInt(10)
    }
    str
  }
}