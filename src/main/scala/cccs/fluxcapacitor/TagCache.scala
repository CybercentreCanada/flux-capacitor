package cccs.fluxcapacitor

import com.google.common.hash.BloomFilter
import scala.collection.mutable.Set

trait TagCache {
  def put(tagName: String): Unit
  def get(tagName: String): Boolean
}

class BloomTagCache(val bloom: BloomFilter[CharSequence]) extends TagCache {
  def put(key: String): Unit = {
    bloom.put(key)
  }
  def get(key: String): Boolean = {
    bloom.mightContain(key)
  }
}

class MapTagCache extends TagCache {
  val tags = Set[String]()
  def put(key: String): Unit = {
    tags += key
  }
  def get(key: String): Boolean = {
    tags(key)
  }

  override def toString(): String = {
    tags.toString()
  }
}
