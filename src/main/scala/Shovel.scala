import Main.Config
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Scan, Table}
import collection.JavaConverters.bufferAsJavaListConverter
import collection.mutable.ListBuffer
import ch.qos.logback.classic.Level
import org.slf4j.LoggerFactory

object Shovel {

  val BATCH_SIZE : Int = 1000
  val logger = LoggerFactory.getLogger("shovel.logger")

  private def getConnection(c: Config): Connection = {
    val config = HBaseConfiguration.create()
    config.set("hbase.client.pause", "100")
    config.set("hbase.client.max.perserver.tasks", "25")
    config.set("hbase.client.max.perregion.tasks", "5")
    if (c.zookeeperQuorum.isDefined)
      config.set("hbase.zookeeper.quorum", c.zookeeperQuorum.get)
    if (c.zookeeperZnodeParent.isDefined)
      config.set("zookeeper.znode.parent", c.zookeeperZnodeParent.get)
    ConnectionFactory.createConnection(config)
  }

  def run(c: Config): Unit = {
    val connection = getConnection(c)
    val table1 = connection.getTable(TableName.valueOf(c.sourceTable))
    val table2 = connection.getTable(TableName.valueOf(c.targetTable))

    logger.asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.DEBUG)
    logger.debug(s"network IDs map = ${c.networkIdsMap}")
    logger.debug(s"struct IDs map = ${c.structIdsMap}")

    val handler = RowKeyHandler(c.networkIdsMap, c.structIdsMap)
    copyToTable(table1, table2, c.networkId, handler.transform)
  }

  private def copyToTable(sourceTable: Table,
                          targetTable: Table,
                          filterNetworkId : Option[Long],
                          transformRow: String => String): Unit = {

    val scan = new Scan()
    if (filterNetworkId.isDefined) {
      val prefix = (filterNetworkId.toString + "_").getBytes
      scan.setRowPrefixFilter(prefix)
    }

    var rowsCopied = 0

    val scanner = sourceTable.getScanner(scan)
    var results = scanner.next(BATCH_SIZE)

    while (!results.isEmpty) {

      val puts = results.map(result => {
        val rowId = new String(result.getRow)
        val newRowId = transformRow(rowId)
        val put = new Put(newRowId.getBytes())

        logger.debug(s"$rowId -> $newRowId")

        for (cell <- result.rawCells()) {
          put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell))
        }
        put
      })

      targetTable.put(puts.to[ListBuffer].asJava)

      rowsCopied += results.length
      logger.info(s"Rows copied: $rowsCopied")

      results = scanner.next(BATCH_SIZE)
    }

    scanner.close()

  }
}
