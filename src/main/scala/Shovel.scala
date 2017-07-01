import Main.Config
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Scan}
import collection.JavaConverters.bufferAsJavaListConverter
import collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

case class Shovel(config: Config) {

  private val BATCH_SIZE = 1000
  private val logger = LoggerFactory.getLogger("shovel.logger")

  def run(): Unit = {
    val connection = getConnection
    logger.debug("connected to hbase")
    copyData(connection)
    connection.close()
  }

  private def getConnection: Connection = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.client.pause", "100")
    hbaseConfig.set("hbase.client.max.perserver.tasks", "25")
    hbaseConfig.set("hbase.client.max.perregion.tasks", "5")
    if (config.zookeeperQuorum.isDefined)
      hbaseConfig.set("hbase.zookeeper.quorum", config.zookeeperQuorum.get)
    if (config.zookeeperZnodeParent.isDefined)
      hbaseConfig.set("zookeeper.znode.parent", config.zookeeperZnodeParent.get)
    ConnectionFactory.createConnection(hbaseConfig)
  }

  private def copyData(connection: Connection): Unit = {
    val sourceTable = connection.getTable(TableName.valueOf(config.sourceTable))
    val targetTable = connection.getTable(TableName.valueOf(config.targetTable))

    logger.debug(s"network IDs map = ${config.networkIdsMap}")
    logger.debug(s"struct IDs map = ${config.structIdsMap}")

    val handler = RowKeyHandler(config.networkIdsMap, config.structIdsMap)

    val scan = new Scan()
    if (config.networkId.isDefined) {
      val prefix = (config.networkId.toString + "_").getBytes
      scan.setRowPrefixFilter(prefix)
    }

    var rowsCopied = 0
    val scanner = sourceTable.getScanner(scan)
    var results = scanner.next(BATCH_SIZE)

    while (!results.isEmpty) {

      logger.info(s"Rows copied: $rowsCopied")

      val puts = results.map(result => {
        val rowId = new String(result.getRow)
        val newRowId = handler.transform(rowId)
        val put = new Put(newRowId.getBytes())

        logger.debug(s"$rowId -> $newRowId")

        for (cell <- result.rawCells()) {
          put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell))
        }
        put
      })

      targetTable.put(puts.to[ListBuffer].asJava)
      rowsCopied += results.length
      results = scanner.next(BATCH_SIZE)
    }

    logger.info(s"Finished, rows copied: $rowsCopied")

    scanner.close()

  }
}
