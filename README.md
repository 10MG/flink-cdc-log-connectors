# CDC Log Connectors for Apache Flink<sup>®</sup>

flink-cdc-log-connectors是基于flink-cdc-connectors改造的一组Apache Flink<sup>®</sup>源连接器。flink-cdc-log-connectors集成了Debezium作为捕获数据变更的引擎，因此它可以充分利用Debezium的能力。使用它能够获取到flink-cdc-connectors所不支持的`op`属性，并能够与flink-cdc-connectors并列使用。

本自述文件旨在简要介绍用于Apache Flink<sup>®</sup>的CDC Log连接器的核心功能。它的使用方法除了连接器不同以外，其它跟flink-cdc-connectors完全一样。有关flink-cdc-connectors的完整详细的文档，参见[flink-cdc-connectors文档](https://ververica.github.io/flink-cdc-connectors/master/)。

## 特性

1. 支持读取数据库快照，并继续读取事务日志，即使出现故障，也可以达到**有且只有一次处理**的效果。
2. 用于DataStream API的CDC Log连接器，用户可以在单个作业中使用多个数据库和表的变更数据，而无需部署Debezium和Kafka。
3. 用于Table/SQL API的CDC Log连接器，用户可以使用SQL和DDL创建CDC源以监视单个表上的变更。

## Table/SQL API的用法

我们需要几个步骤使用提供的连接器来设置Flink集群

1. 安装1.12+和Java 8+版本的Flink集群。
2. 从[下载页面](https://mvnrepository.com/artifact/cn.tenmg)下载SQL连接器的jar（或自行构建）。
3. 将下载的jar放在`FLINK_HOME/lib/`下。
4. 重新启动Flink集群。

该示例显示了如何在[Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/dev/table/sqlClient.html)中创建MySQL CDC Log源并对其执行查询。

```sql
-- creates a mysql cdc table source
CREATE TABLE mysql_binlog (
 id INT NOT NULL,
 name STRING,
 description STRING,
 weight DECIMAL(10,3)
) WITH (
 'connector' = 'mysql-cdc-log',
 'hostname' = 'localhost',
 'port' = '3306',
 'username' = 'flinkuser',
 'password' = 'flinkpw',
 'database-name' = 'inventory',
 'table-name' = 'products'
);

-- 从MySQL读取快照和Binlog数据，进行一些转换，并在客户端上显示
SELECT id, UPPER(name), description, weight FROM mysql_binlog;
```

## DataStream API的用法

包括以下Maven依赖项（可通过Maven 中央仓库获得）。

```
<dependency>
  <groupId>cn.tenmg</groupId>
  <!-- 添加与数据库匹配的依赖项 -->
  <artifactId>flink-connector-mysql-cdc-log</artifactId>
  <!-- 该依赖项仅适用于稳定版本，镜像版需要自己构建。请将${flink-connector-cdc-log.version}替换为所使用的版本号，也可通过pom属性定义。 -->
  <version>${flink-connector-cdc-log.version}</version>
</dependency>
```

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import cn.tenmg.cdc.log.debezium.JsonDebeziumDeserializationSchema;
import MySqlSource;

public class MySqlSourceExample {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("yourHostname")
            .port(yourPort)
            .databaseList("yourDatabaseName") // 设置要捕获的数据库
            .tableList("yourDatabaseName.yourTableName") // 设置要捕获的表名
            .username("yourUsername")
            .password("yourPassword")
            .deserializer(new JsonDebeziumDeserializationSchema()) // 将SourceRecord转换为JSON字符串
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 启用检查点
    env.enableCheckpointing(3000);

    env
      .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
      // 设置4个并行源任务
      .setParallelism(4)
      .print().setParallelism(1); // 接收器使用1个并行度以保持消息顺序

    env.execute("Print MySQL Snapshot + Binlog");
  }
}
```

## 使用源码编译

- 环境准备：
    - git
    - Maven
    - Java 8及以上

```
git clone https://gitee.com/tenmg/flink-cdc-log-connectors.git
cd flink-cdc-log-connectors
mvn clean install -DskipTests
```

编译完成后，依赖项在您的Maven本地仓库`.m2`中，或者在各子项目对应的target目录下，可添加到项目中使用。

## 许可证

该仓库使用 [Apache Software License 2](https://github.com/ververica/flink-cdc-connectors/blob/master/LICENSE)开源许可证。

## 参与贡献

欢迎以任何人以任何方式提供帮助，无论是报告问题、帮助编写文档，还是提供代码修改以修复错误、添加测试或实现新功能。您可以在[Gitee Issues](https://gitee.com/tenmg/flink-cdc-log-connectors/issues)中报告问题以请求功能或新特性。

## 文档

更多用法和说明可参考 [https://ververica.github.io/flink-cdc-connectors](https://ververica.github.io/flink-cdc-connectors)，flink-cdc-log-connectors除了使用的类或者连接器不一样之外，其他用法与flink-cdc-log-connectors完全一样。
