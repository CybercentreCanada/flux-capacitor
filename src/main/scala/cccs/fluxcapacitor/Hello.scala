package cccs.fluxcapacitor

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoders

object Hello {
  def main(args: Array[String]) = {
    println("Hello, world")

    val f = FluxState()
    f.tagCapacity = 200000
    f.useFluxStore = false
    f.desiredFpp = 0.0001
    f.init()
    f.toState()

    //   val enc = Encoders.product[FluxState].asInstanceOf[ExpressionEncoder[FluxState]]
    //   val serializer = enc.createSerializer()
    // val start = System.currentTimeMillis()
    // for(i <- Range(1, 500)){
    //   val row = serializer(f)
    // }
    // val end = System.currentTimeMillis()
    // print(s"took ${end - start}")
    print("done")

  }
}
