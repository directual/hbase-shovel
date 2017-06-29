/**
  * Created by yuriy on 29.06.17.
  */


object RawKey {
  def apply(networkId : Long, structId : Long, objectKey: String): String = {
    s"${networkId.toString}_${structId}_$objectKey"
  }

  def unapply(arg: String): Option[(Long, Long, String)] = {
    val pattern = "([0-9]+)_([0-9]+)_(.*)".r

    try {
      val pattern(x, y, z) = arg
      Option(x.toLong, y.toLong, z)
    } catch {
      case _ : Exception => None
    }
  }
}


case class RowKeyHandler(networkIdMap_ : Map[Long, Long], structIdMap_ : Map[Long, Long]) {
  private val networkIdMap = networkIdMap_
  private val structIdMap = structIdMap_

  private def structIdTransform(id : Long) : Long = {
    if (structIdMap.contains(id)) structIdMap(id) else id
  }

  private def networkIdTransform(id : Long) : Long = {
    if (networkIdMap.contains(id)) networkIdMap(id) else id
  }

  def transform(rawKey : String) : String = {
    rawKey match {
      case RawKey(x, y, z) => RawKey(networkIdTransform(x), structIdTransform(y), z)
      case _ => rawKey
    }
  }
}
