package cs.luc.edu

package object timing {

  case class Time(t: Double) {
    val nanoseconds = t.toLong
    val milliseconds = (t / 1.0e6).toLong

    def +(another: Time): Time = Time(t + another.t)

    override def toString(): String = f"Time(t=$t%.2f, ns=$nanoseconds%d, ms=$milliseconds%d)";
  }

  // time a block of Scala code - useful for timing everything!
  // return a Time object so we can obtain the time in desired units

  def nanoTime[R](block: => R): (Time, R) = {
    val t0 = System.nanoTime()
    // This executes the block and captures its result
    // call-by-name (reminiscent of Algol 68)
    val result = block
    val t1 = System.nanoTime()
    val deltaT = t1 - t0
    (Time(deltaT), result)
  }
}