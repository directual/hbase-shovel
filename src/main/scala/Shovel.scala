package com.directual.hbase.shovel

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import collection.JavaConverters.bufferAsJavaListConverter
import collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
import Main.Config
import ActionType._

case class Shovel(config: Config) {

  private val BatchSize = 500
  private val logger = LoggerFactory.getLogger("shovel.logger")

  def run(): Unit = {
    val connection = getConnection
    logger.debug("connected to hbase")
    handleData(connection)
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

  private def handleData(connection: Connection): Unit = {
    val sourceTable = connection.getTable(TableName.valueOf(config.sourceTable))
    val targetTable =
      if (config.action == Copy)
        Some(connection.getTable(TableName.valueOf(config.targetTable.get)))
      else
        None

    logger.debug(s"action = ${config.action}")
    logger.debug(s"network IDs map = ${config.networkIdsMap}")
    logger.debug(s"struct IDs map = ${config.structIdsMap}")

    val handler = RowKeyHandler(config.networkIdsMap, config.structIdsMap)

    val scan = new Scan()
    if (config.networkId.isDefined) {
      val prefix = (config.networkId.get + "_").getBytes
      scan.setRowPrefixFilter(prefix)
    }

    var rowsProcessed = 0
    val scanner = sourceTable.getScanner(scan)
    var results = scanner.next(BatchSize)

    while (!results.isEmpty) {
      logger.info(s"Rows processed $rowsProcessed")
      config.action match {
        case Copy =>
          copyRows(results, targetTable.get, handler)
        case Remove =>
          removeRows(results, sourceTable)
      }
      rowsProcessed += results.length
      results = scanner.next(BatchSize)
    }

    logger.info(s"Rows processed $rowsProcessed")
    logger.info("Finished")

    scanner.close()
  }

  private def copyRows(results: Array[Result], targetTable: Table, handler: RowKeyHandler): Unit = {
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
  }

  private def removeRows(results: Array[Result], targetTable: Table): Unit = {
    val deletes = results.map(result => {
      val rowId = new String(result.getRow)
      val delete = new Delete(rowId.getBytes())

      logger.debug(s"deleting $rowId")

      for (cell <- result.rawCells()) {
        delete.addColumns(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell))
      }
      delete
    })
    targetTable.delete(deletes.to[ListBuffer].asJava)
  }
}
