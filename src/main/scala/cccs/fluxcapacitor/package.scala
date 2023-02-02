package cccs

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

package object fluxcapacitor {
  // CCCS: Class representing a row value, spark puts the state inside a row with a "groupState" column.
  case class FluxGroupState(groupState: FluxState)

  implicit val fluxGroupStateEnc = Encoders
    .product[FluxGroupState]
    .asInstanceOf[ExpressionEncoder[FluxGroupState]]
  implicit val fluxGroupStateSerializer = fluxGroupStateEnc.createSerializer()
  implicit val keyEncoder = ExpressionEncoder[String]
  implicit val keySerializer = keyEncoder.createSerializer()

  // CCCS: FluxState fields
  val VERSION_INDEX = 0
  val ACTIVE_INDEX = 1
  val SERIALIZED_BLOOMS_INDEX = 2
  val PUTCOUNT_INDEX = 3
  val GETCOUNT_INDEX = 4
  val SORT_TIMER_INDEX = 5
  val UPDATETAGCACHE_TIMER_INDEX = 6
  val FROMSTATE_TIMER_INDEX = 7
  val TOSTATE_TIMER_INDEX = 8
  val FLUXSTATE_NUM_FIELDS = 9

  // CCCS: We hard code the number of blooms
  val NUM_BLOOMS = 10

  def getFluxStateRow(groupStateRow: UnsafeRow) = groupStateRow.getStruct(0, FLUXSTATE_NUM_FIELDS)
  def getVersion(row: UnsafeRow) = getFluxStateRow(row).getInt(VERSION_INDEX)
  def getActive(row: UnsafeRow) = getFluxStateRow(row).getInt(ACTIVE_INDEX)
  def getBloomArray(row: UnsafeRow) = getFluxStateRow(row).getArray(SERIALIZED_BLOOMS_INDEX)

  def getActiveValue(row: UnsafeRow) = getFluxStateRow(row).getInt(ACTIVE_INDEX)
  def getGetTagCount(row: UnsafeRow) = getFluxStateRow(row).getLong(GETCOUNT_INDEX)
  def getPutTagCount(row: UnsafeRow) = getFluxStateRow(row).getLong(PUTCOUNT_INDEX)
  def getSortTime(row: UnsafeRow) = getFluxStateRow(row).getLong(SORT_TIMER_INDEX)
  def getUpdateTagCacheTime(row: UnsafeRow) = getFluxStateRow(row).getLong(UPDATETAGCACHE_TIMER_INDEX)
  def getFromStateTimer(row: UnsafeRow) = getFluxStateRow(row).getLong(FROMSTATE_TIMER_INDEX)
  def getToStateTimer(row: UnsafeRow) = getFluxStateRow(row).getLong(TOSTATE_TIMER_INDEX)

  // CCCS
  def makeRowKey(groupKey:String, bloomIndex:Int) = keySerializer(groupKey + bloomIndex).asInstanceOf[UnsafeRow]
  // CCCS
  def makeRowValue(fluxState:FluxState) = fluxGroupStateSerializer(FluxGroupState(fluxState)).asInstanceOf[UnsafeRow]

    // CCCS: deserialize a row representing a FluxState
    def deserializeFluxState(value: UnsafeRow): FluxState = {
      // The FluxState struct has 4 fields, get each of them
      var version = getVersion(value)
      var active = getActive(value)
      var unsafeBloomArray = getBloomArray(value)
      // When storing state we get only 1 bloom to store, get that serialized bloom
      var modifiedSerializedBloom = unsafeBloomArray.getBinary(0)
      // We can now create a row to store
      FluxState(version, active, List(modifiedSerializedBloom))
    }

    // CCCS: Coalesce the values we put into the store
    // We need to find which bloom is the active one, we do this based on the versions
    def coalesceFluxStates(fluxStates: Seq[FluxState], startTime: Long): FluxState = {
      val binBloomArray = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
      var latestActive = -1
      var latestVersion = -1L
      for(fluxState <- fluxStates) {
        binBloomArray += fluxState.serializedBlooms(0)
        if (fluxState.version > latestVersion) {
            latestVersion = fluxState.version
            latestActive = fluxState.active
        }
      }
      FluxState(latestVersion, latestActive, binBloomArray.toList, fromStateTimer = startTime)
    }

}
