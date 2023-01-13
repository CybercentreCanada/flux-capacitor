package cccs.fluxcapacitor

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.apache.log4j.Logger

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.collection.mutable.ArrayBuffer

import scala.util.control.Breaks._


case class FluxState(
    var version: Long = 0,
    var active: Int = 0,
    var serializedBlooms: List[Array[Byte]] = List(),
    var tagCount: Long = 0
) extends TagCache {

  val log = Logger.getLogger(this.getClass)

  @transient var blooms: List[BloomFilter[CharSequence]] = List()
  @transient val desiredFpp = 0.0001
  @transient val NUM_BLOOMS = 10
  @transient var tagCapacity = 0
  @transient var useFluxStore = false

  override def put(tagName: String): Unit = {
    tagCount += 1L
    getActiveBloom().put(tagName)
  }


  override def get(tagName: String): Boolean = {
    val numBloomsToCheck = blooms.length - 1
    val start = active 
    val end = active - numBloomsToCheck
    log.debug("========")
    log.debug(s"number of blooms ${blooms.length}")
    log.debug(s"active bloom is $active")
    var mightContain = false
    for(bloomIndex <- start to end by -1) {
        var i = bloomIndex
        if (bloomIndex < 0) {
          i = bloomIndex + NUM_BLOOMS
        }
        if (i >= blooms.length){
          log.debug(s"getting from bloom $i")
          break
        }
        log.debug(s"getting from bloom $i")
        //mightContain = blooms(i).mightContain(tagName)
        if (mightContain == true){
          break
        }
    }
    mightContain
  }

  def init(): FluxState = {
    val bloom = createBloom()
    blooms = List(bloom)
    this
  }

  def toState(): FluxState = {
    version += 1
    // we make sure to pre-allocate the output buffer, this has drastic measured performance improvement
    // in an experiment each micro-batch took 80 seconds, with pre-allocated buffer it went down to 30 seconds.
    // Doing this also help JVM GC pressure, there is way less intermediary arrays created while it grows.
    // We know approximately the size of the bloom in bytes so we can easily pre-allocate the buffer.
    // Note the size depends on the false positive
    serializedBlooms = 
      if (useFluxStore) {
        // we only store the active blooms, all other blooms were un-changed
        val approxsize = tagCapacity / NUM_BLOOMS * 3
        val byteArrayOut = new ByteArrayOutputStream(approxsize)
        val store = new ObjectOutputStream(byteArrayOut)
        store.writeObject(getActiveBloom())
        store.close
        List(byteArrayOut.toByteArray())
      } else {
        blooms.map(bloom => {
        val approxsize = tagCapacity / NUM_BLOOMS * 3
        val byteArrayOut = new ByteArrayOutputStream(approxsize)
        val store = new ObjectOutputStream(byteArrayOut)
        store.writeObject(bloom)
        store.close
        byteArrayOut.toByteArray()
        })
      }
    val fpp = getActiveBloom().expectedFpp()
    log.debug(
      f"preparing for storage, active bloom is $active and it's fpp is $fpp%1.8f, num puts: $tagCount"
    )
    this
  }

  def getActiveBloom(): BloomFilter[CharSequence] = {
    blooms(active)
  }

  def cycleBloom(): Unit = {
    log.debug("cycling blooms")
    active = (active + 1) % NUM_BLOOMS
    val newBloom = createBloom()
    if (blooms.length == NUM_BLOOMS) {
      log.debug(s"replacing bloom at index $active")
      val buff = blooms.to[ArrayBuffer]
      buff(active) = newBloom
      blooms = buff.toList
    } else {
      log.debug("adding a bloom to the list")
      blooms = blooms ++ List(newBloom)
    }
    val fpp = getActiveBloom().expectedFpp()
    log.debug(f"active bloom is $active and it's fpp is $fpp%1.8f")
  }

  def fromState(): FluxState = {
    blooms = serializedBlooms
      .map(theBytes => {
        val byteArrayInput = new ByteArrayInputStream(theBytes)
        val input = new ObjectInputStream(byteArrayInput)
        val obj = input.readObject()
        obj.asInstanceOf[BloomFilter[CharSequence]]
      })

    log.debug(s"number of blooms deserialized from store cache: ${blooms.length}")
    
    val fpp = getActiveBloom().expectedFpp()
    if (fpp > desiredFpp) {
      log.debug(f"active bloom $active is full. fpp $fpp%1.8f > $desiredFpp")
      cycleBloom()
    }
    else if (version % 5 == 0) {
      log.debug(f"trigger bloom cycle, just for testing!")
      cycleBloom()
    }

    this
  }

  def createBloom() = {
    val bloomCapacity: Int = tagCapacity / NUM_BLOOMS
    val bloom = BloomFilter.create(Funnels.stringFunnel(), bloomCapacity, desiredFpp)
    val prep = (bloomCapacity * 0.95).toInt
    (1 to prep).map("padding" + _).foreach(s => bloom.put(s))
    bloom
  }

}