
object RowKey {
  def apply(networkId: Long, structId: Long, objectKey: String): String = {
    s"${networkId}_${structId}_$objectKey"
  }

  def unapply(arg: String): Option[(Long, Long, String)] = {
    val pattern = "([0-9]+)_([0-9]+)_(.*)".r

    try {
      val pattern(x, y, z) = arg
      Option(x.toLong, y.toLong, z)
    } catch {
      case _: Exception => None
    }
  }
}

case class RowKeyHandler(networkIdsMap: Map[Long, Long], structIdsMap: Map[Long, Long]) {

  private def structIdTransform(id: Long): Long = {
    if (structIdsMap.contains(id)) structIdsMap(id) else id
  }

  private def networkIdTransform(id: Long): Long = {
    if (networkIdsMap.contains(id)) networkIdsMap(id) else id
  }

  def transform(rowKey: String): String = rowKey match {
    case RowKey(x, y, z) => RowKey(networkIdTransform(x), structIdTransform(y), z)
    case _ => rowKey
  }
}
