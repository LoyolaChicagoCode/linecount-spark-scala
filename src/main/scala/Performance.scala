package cs.luc.edu

package object performance {

  case class Time(t: Double) {
    val nanoseconds = t.toLong
    val milliseconds = (t / 1.0e6).toLong

    def +(another: Time): Time = Time(t + another.t)

    override def toString(): String = f"Time(t=$t%.2f, ns=$nanoseconds%d, ms=$milliseconds%d)";
  }

  case class Space(m: Long) {
    val memUsed = m.toDouble
    val memUsedGB = memUsed / math.pow(1024.0, 3)
    val totalMemory = Runtime.getRuntime().totalMemory
    val totalGB = totalMemory / math.pow(1024.0, 3)
    val freeMemory = Runtime.getRuntime().freeMemory
    val freeGB = totalMemory / math.pow(1024.0, 3)

    override def toString(): String = f"Space(memUsedGB=$memUsedGB%.2f, free=$freeGB%.2f, total=$totalGB%.2f)";
  }
  // time a block of Scala code - useful for timing everything!
  // return a Time object so we can obtain the time in desired units

  def performance[R](block: => R): (Time, Space, R) = {
    val t0 = System.nanoTime()
    val m0 = Runtime.getRuntime.freeMemory
    // This executes the block and captures its result
    // call-by-name (reminiscent of Algol 68)
    val result = block
    val t1 = System.nanoTime()
    val m1 = Runtime.getRuntime.freeMemory
    val deltaT = t1 - t0
    val deltaM = m0 - m1
    (Time(deltaT), Space(deltaM), result)
  }
}
