

CREATE TABLE MyTable(
                      database varchar,
                      table varchar as tablename,
                      type varchar as ctype,
  data.ID varchar as id
)WITH(
  type ='kafka',
  bootstrapServers ='fjr-yz-204-15:9092,fjr-yz-0-134:9092,fjr-yz-0-135:9092',
  zookeeperQuorum ='fjr-yz-0-140:2181',
  offsetReset ='latest',
  groupId='g002',
  topic ='BINLOG_ASSET_TB_INVEST',
  timezone='Asia/Shanghai',
  parallelism ='4'
);



CREATE TABLE SinkKafka(
                        id varchar,
                        tablename varchar,
                        ctype varchar,
                        PRIMARY KEY(id)
)WITH(
  type ='kafka',
  bootstrapServers ='fjr-yz-204-15:9092,fjr-yz-0-134:9092,fjr-yz-0-135:9092',
  topic ='FLINK_SINK',
  parallelism ='4'
);


insert into SinkKafka
select
  id ,tablename,ctype from MyTable;