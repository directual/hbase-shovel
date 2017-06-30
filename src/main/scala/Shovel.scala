import Main.Config
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan, Table}


object Shovel {

  def run(c : Config): Unit = {
    val config = HBaseConfiguration.create()
    if (!c.zookeeperQuorum.isEmpty)
      config.set("hbase.zookeeper.quorum", c.zookeeperQuorum)
    config.set("hbase.client.pause", "100")
    config.set("hbase.client.max.perserver.tasks", "25")
    config.set("hbase.client.max.perregion.tasks", "5")
    if (!c.zookeeperZnodeParent.isEmpty)
      config.set("zookeeper.znode.parent", c.zookeeperZnodeParent)
    val connection = ConnectionFactory.createConnection(config)
    val table1 = connection.getTable(TableName.valueOf(c.sourceTable))
    val table2 = connection.getTable(TableName.valueOf(c.targetTable))

    println("map = " + c.structIdsMap)

    val scan = new Scan()
    if (c.networkId != -1) {
      val prefix = (c.networkId.toString + "_").getBytes
      scan.setRowPrefixFilter(prefix)
    }

    val handler = RowKeyHandler(c.networkIdsMap, c.structIdsMap)
    copyToTable(table1, table2, handler.transform, scan)
  }

  def copyToTable(sourceTable : Table, targetTable : Table, transformRow : String => String, scan : Scan) : Unit = {

    val scanner = sourceTable.getScanner(scan)
    var result = scanner.next()

    while (result != null) {
      val rowId = new String(result.getRow)
      val put = new Put(transformRow(rowId).getBytes())

      println(rowId, transformRow(rowId))

      for(cell <- result.rawCells()) {
        put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell))
      }
      targetTable.put(put)

      result = scanner.next()
    }

    scanner.close()

  }
}
