# hbase-shovel

Making executable jar

### Making executable jar
```bash
sbt clean assembly
```

### Example
```bash
java -jar target/scala-2.12/hbase-shovel-assembly-1.0.jar --input table1 --output table2 --structIdsMap 0=1,1=2,2=3
```
