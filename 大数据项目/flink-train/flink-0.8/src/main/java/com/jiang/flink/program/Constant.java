package com.jiang.flink.program;
/**
 * @Author jiang.li
 * @Description //TODO 常量配置项
 * @Date 14:52 2019-11-12
 * @Param
 * @return
 **/
public interface Constant {
    //设置用户自定义配置文件的文件名称
    String APPCONFFILENAME = "application.properties";

    //凤金雷达，运营引擎，社区概况数据需求
    String BOOTSTRAPSERVER = "bootstrap.server";
    String FENGJR_BBS_DATABASE = "fengjrbbs";
    String NEED_TABLES = "bbs_post,bbs_post_likes,bbs_post_reply,bbs_post_reply_likes,bbs_post_shares,bbs_post_views,bbs_user_login_info";
    String RANK_TABLE = "bbs_post";

    String OPERATECOMMUNITYOVERVIEWCONSUMERGROUP = "operate.community.overview.comsumer.group";

    String OPERATECOMMUNITYOVERVIEWTOPIC = "operate.community.overview.topic";

    String ZOOKEEPERQUORUM = "zookeeper.qurum";
    String ZOOKEEPERZNODEPARENT = "zookeeper.znode.parent";

    //kerberos环境相关
    String KERBEROS_KEYTABFILE_NAME = "hdfs.keytab";
    String KERBEROS_MASTERPRIN = "hbase/_HOST@hadoop_edw";
    String KERBEROS_REGIONPRIN = "hbase/_HOST@hadoop_ed";
    String KERBEROS_USERNAME = "hdfs@hadoop_edw";

    //关于rocksDB
    String ROCKDB_BACKEND_STATE_URL = "rocksdb.state.backend.url";

    String KEYTAB_HDFS_PATH = "keytab.hdfs.path";

    //关于kafka
    String BINLOG_JSON_ISDDL_FIELD = "isDdl";
    String BINLOG_JSON_TABLENAME_FIELD = "table";
    String BINLOG_JSON_DATABASENAME_FIELD = "database";
    String BINLOG_JSON_DATA_FIELD = "data";
    String BINLOG_JSON_DATA_CREATE_FIELD = "create_time";

    //关于环境
    String ENVIRONMENT = "appenv";

    //关于Hbase
    String COMMONFAMILY = "d";


}
