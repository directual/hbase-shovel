import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Table, Scan, Put, ConnectionFactory}

object Main extends App {

  case class Config(sourceTable : String = "",
                    targetTable : String = "",
                    zookeeperQuorum : String = "",
                    znodeParent : String = "",
                    networkId : Long = -1,
                    networkIdsMap : Map[Long, Long] = Map(),
                    structIdsMap : Map[Long, Long] = Map())

  val parser = new scopt.OptionParser[Config]("hbase-shovel") {
    head("hbase-shovel")

    opt[String]('i', "input")
      .valueName("<sourceTable>")
      .action((value, config) => config.copy(sourceTable = value))
      .required()
    opt[String]('o', "output")
      .valueName("<targetTable>")
      .action((value, config) => config.copy(targetTable = value))
      .required()
    opt[String]('z', "zookeeperQuorum")
      .valueName("<hbase.zookeeper.quorum>")
      .action((value, config) => config.copy(zookeeperQuorum = value))
    opt[String]('p', "znode-parent")
      .valueName("<zookeeper.znode.parent>")
      .action((value, config) => config.copy(znodeParent = value))
    opt[Long]('f', "networkId")
      .valueName("<network id>")
      .action((x, config) => config.copy(networkId = x))
      .validate( x =>
        if (x >= 0) success
        else failure("Value <network id> must be non-negative")
      )
      .text("filter networkId")
    opt[Map[Long, Long]]('n', "networkIdsMap")
      .valueName("<id1=id2,id3=id4...>")
      .action((x, config) => config.copy(networkIdsMap = x))
    opt[Map[Long, Long]]('s', "structIdsMap")
      .valueName("<id1=id2,id3=id4...>")
      .action((x, config) => config.copy(structIdsMap = x))
  }

  parser.parse(args, Config()) match {
    case Some(config) =>
      run(config)
    case None =>
      sys.exit(1)
  }

  def run(c : Config): Unit = {
    val config = HBaseConfiguration.create()
    if (c.zookeeperQuorum.length > 0)
      config.set("hbase.zookeeper.quorum", c.zookeeperQuorum)
    config.set("hbase.client.pause", "100")
    config.set("hbase.client.max.perserver.tasks", "25")
    config.set("hbase.client.max.perregion.tasks", "5")
    if (c.znodeParent.length > 0)
      config.set("zookeeper.znode.parent", c.znodeParent)
    val connection = ConnectionFactory.createConnection(config)
    val table1 = connection.getTable(TableName.valueOf(c.sourceTable))
    val table2 = connection.getTable(TableName.valueOf(c.targetTable))

    println("map = " + c.structIdsMap)

    val scan = new Scan()
    if (c.networkId >= 0) {
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
