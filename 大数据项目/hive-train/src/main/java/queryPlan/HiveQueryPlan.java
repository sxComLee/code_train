package queryPlan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName HiveQueryPlan
 * @Description TODO hive查询执行计划和Job数量
 * @Author jiang.li
 * @Date 2020-01-02 16:42
 * @Version 1.0
 */
public class HiveQueryPlan {
    public static void main(String[] args) throws Exception {
//        String sql = "CREATE TABLE liuxiaowen.lxw1234 AS SELECT * FROM liuxiaowen.lxw1";
//        String sql2 = "INSERT OVERWRITE TABLE liuxiaowen.lxw3 SELECT a.url FROM liuxiaowen.lxw1 a join liuxiaowen.lxw2 b ON (a.url = b.domain)";
//
        String sqls ="insert overwrite table dwi.dwi_user_fortune_tb_invest_customer_attr\n" +
                "select\n" +
                "base.id as `客户Id`\n" +
                ",base.cast_in_invenst_amount as `持有网贷定期金额`\n" +
                ",nvl(fplan.stay_for,0)+nvl(orl.amount,0)+nvl(pfun.amount,0) as `持有财富金额`\n" +
                ",nvl(ove.hold,0) as `持有海外金额（美元）`\n" +
                ",nvl(orcl_for1.amount,0)+nvl(orcl_for2.amount,0)+nvl(orcl_for3.amount,0)  as `持有基金（券商）金额`\n" +
                ",nvl(repay.to_mon_amount,0) as `当月回款网贷定期`\n" +
                ",nvl(fplan.mon_for,0)+nvl(devide.amount,0)+nvl(rede.quit,0)+nvl(rede.get,0) as `当月财富回款金额`\n" +
                ",nvl(ove1.amount,0) as `当月海外赎回金额（美元）`\n" +
                ",nvl(sch.amount,0) as `当月基金（券商）回款金额`\n" +
                ",nvl(ord.amount,0)+nvl(orcl.bala,0) as `累计投资基金（券商）`\n" +
                ",nvl(base.total_invenst_amount,0) as `累计投资网贷&定期`\n" +
                ",nvl(base.total_invenst_wealth_amount,0) as `累计投资财富`\n" +
                ",nvl(base.total_invenst_overseas_amount,0) as `累计投资海外金额`\n" +
                ",'' as `ext1`\n" +
                ",'' as `ext2`\n" +
                ",'' as `所属城市名称`\n" +
                ",'' as `所属组名称`\n" +
                "from\n" +
                "    (select id,cast_in_invenst_amount,total_invenst_amount,total_invenst_wealth_amount,total_invenst_overseas_amount,user_id from dwi.dwi_user_fortune_tb_invest_customers ) base\n" +
                "left join\n" +
                "    (select\n" +
                "         user_id,\n" +
                "         sum(if(status='UNDUE' and concat(year(duedate),month(duedate))=concat(year(current_date()),month(current_date())) ,nvl(amountinterest,0)+nvl(amountprincipal,0),\n" +
                "             if(status='REPAYED' and concat(year(repaydate),month(repaydate))=concat(year(current_date()),month(current_date())) ,nvl(repayamount,0),null))) as to_mon_amount\n" +
                "        from dwi.dwi_user_fortune_ordr_repay_detail\n" +
                "        where source_type in ('biz','fengchu','asset') group by user_id\n" +
                "    ) repay\n" +
                "    on base.user_id=repay.user_id\n" +
                "left join\n" +
                "    (      select uid, hold_overseas_amount as hold from ods.wealth_sales_tb_invest_overseas_full where dt=date_sub(current_date,1)) ove\n" +
                "    on ove.uid=base.user_id\n" +
                "left join\n" +
                "    (\n" +
                "    SELECT\n" +
                "    a.online_user_id as user_id ,\n" +
                "    --`持有财富金额\n" +
                "    nvl(sum(if(l.LOAN_ID is null,PREDICT_AMOUNTPRINCIPAL , null )),0) as stay_for,\n" +
                "    --`当月财富回款金额`\n" +
                "    nvl(sum(if(l.LOAN_ID is null and concat(year(current_date()),month(current_date()))=concat(year(CURRENT_REPAYMENT_DATE),month(CURRENT_REPAYMENT_DATE)),nvl(PREDICT_AMOUNTPRINCIPAL,0)+nvl(PREDICT_AMOUNTINTEREST,0) ,null)),0)\n" +
                "    +nvl(sum(if(l.LOAN_ID is not null and concat(year(current_date()),month(current_date()))=concat(year(REAL_REPAYMENT_DATE),month(REAL_REPAYMENT_DATE)),nvl(REAL_AMOUNTPRINCIPAL,0)+nvl(REAL_AMOUNTINTEREST,0), null)),0)\n" +
                "        as mon_for\n" +
                "    FROM\n" +
                "      (select * from dwd.dwd_ordr_fortune_repayment_plan_full where dt = date_sub(current_date(),1) ) AS a\n" +
                "    LEFT JOIN (\n" +
                "      SELECT LOAN_ID,PERIOD_ID,CURRENT_PERIOD\n" +
                "      from ods.fortune_fortune_repayment_plan_exec_log_full\n" +
                "        where dt = date_sub(current_date(),1) and sub_type = 1\n" +
                "        group by loan_id ,period_id,current_period) AS l\n" +
                "    on a.LOAN_ID = l.LOAN_ID and a.PERIOD_ID = l.PERIOD_ID and a.CURRENT_PERIOD = l.CURRENT_PERIOD\n" +
                "    group by a.online_user_id\n" +
                "    ) fplan\n" +
                "    on fplan.user_id=base.user_id\n" +
                "left join\n" +
                "    --私募持仓资产\n" +
                "    (select sum(t.balance * nvl(c.nav, 1) + nvl(t.PAYABLE_PROFIT, 0)) as amount,u.user_id as user_id\n" +
                "        from (select * from ods.fengpe_tr_trust_channel_share_full where dt = date_sub(current_date(),1) and balance > 0 ) t\n" +
                "        left join (\n" +
                "\n" +
                "                   select distinct m.fund_code, f.nav, m.nav_date\n" +
                "                     from (select fund_code, max(t.nav_actual_date) as nav_date\n" +
                "                              from (select * from ods.fengpe_cl_fund_market_full where dt = date_sub(current_date(),1)) t\n" +
                "                             group by fund_code) m\n" +
                "                     left join (select * from ods.fengpe_cl_fund_market_full where dt = date_sub(current_date(),1)) f\n" +
                "                       on m.nav_date = f.nav_actual_date\n" +
                "                      and f.fund_code = m.fund_code\n" +
                "\n" +
                "                   ) c\n" +
                "          on t.fund_code = c.fund_code\n" +
                "      left join (select * from ods.fengpe_ac_trade_account_full where dt = date_sub(current_date(),1)) ac on ac.trade_account_no = t.trade_account_no\n" +
                "      left join (select * from dwd.dwd_user_fund_user_full where dt = date_sub(current_date(),1)) u on u.custno = ac.customer_no\n" +
                "      group by u.user_id)orl\n" +
                " on base.user_id = orl.user_id\n" +
                "left join\n" +
                "    (\n" +
                "    select user_id,sum(if(`trade_type`='2' AND `order_status` = 20,confirmed_amount,null)) as quit,\n" +
                "            sum(if(`trade_type`='1' AND `order_status` in('2','3'),confirmed_amount,null)) as get\n" +
                "    from dwd.dwd_ordr_fortune_pfund_redemption_full where concat(year(current_date()),month(current_date()))=concat(year(confirmed_date),month(confirmed_date))\n" +
                "    group by user_id\n" +
                "    )rede\n" +
                "on base.user_id=rede.user_id\n" +
                "left join\n" +
                "    (\n" +
                "      select user_id,sum(dividend_amount) as amount\n" +
                "       from ods.fortune_pfund_dividends_detail_full where `dividend_method` ='2' and concat(year(current_date()),month(current_date()))=concat(year(confirm_date),month(confirm_date))\n" +
                "       group by user_id\n" +
                "    )devide\n" +
                "on base.user_id=devide.user_id\n" +
                "\n" +
                "left join\n" +
                "    (select sum(t.balance * nvl(c.nav, 1) + nvl(t.PAYABLE_PROFIT, 0)) as amount,u.user_id as user_id\n" +
                "  from (select * from dwd.dwd_user_fund_trust_channel_share_full where dt=date_sub(current_date,1) and balance > 0 ) t\n" +
                "  left join (\n" +
                "\n" +
                "             select distinct m.fund_code, f.nav, m.nav_date\n" +
                "               from (select fund_code, max(t.nav_actual_date) as nav_date\n" +
                "                        from (select * from ods.orcltestdg2_cl_current_fund_market_full where dt=date_sub(current_date,1)) t\n" +
                "                       group by fund_code) m\n" +
                "               left join (select * from ods.orcltestdg2_cl_current_fund_market_full where dt=date_sub(current_date,1)) f\n" +
                "                 on m.nav_date = f.nav_actual_date\n" +
                "                and f.fund_code = m.fund_code\n" +
                "\n" +
                "             ) c\n" +
                "    on t.fund_code = c.fund_code\n" +
                "    left join (select * from dwd.dwd_user_fund_trade_account_full where dt=date_sub(current_date,1)) ac on ac.trade_account_no = t.trade_account_no\n" +
                "    left join (select * from dwd.dwd_user_fund_user_full where dt = date_sub(current_date(),1)) u on u.custno = ac.customer_no\n" +
                "    group by u.user_id\n" +
                "    ) orcl_for1\n" +
                "on orcl_for1.user_id=base.user_id\n" +
                "left join\n" +
                "    (select\n" +
                "    fu.user_id as user_id,\n" +
                "    sum(nvl(aa.balance,0))  as amount\n" +
                "    from (select * from dwd.dwd_user_fund_user_full where dt = date_sub(current_date(),1)) fu\n" +
                "    left join(\n" +
                "        select sum(t.application_amount) as balance, t.custno\n" +
                "        from (\n" +
                "            select * from dwd.dwd_ordr_fund_trade_request_full\n" +
                "            where dt=date_sub(current_date,1)\n" +
                "            and is_parent = '0'\n" +
                "            and payment_state = '1'\n" +
                "            and return_channel_id in ('I', 'K','XJB','8')\n" +
                "            and business_state in ('2050', '2070', '2080')\n" +
                "            and business_type in ('T020', 'T022', 'T039')\n" +
                "        ) t\n" +
                "        group by t.custno\n" +
                "\n" +
                "        union all\n" +
                "\n" +
                "        select sum(a.application_amount) as balance, a.custno\n" +
                "        from (\n" +
                "            select t.* from dwd.dwd_ordr_fund_trade_request_full t\n" +
                "            left join(\n" +
                "                select a.parent_request_id\n" +
                "                from (\n" +
                "                    select * from dwd.dwd_ordr_fund_trade_request_full\n" +
                "                    where dt=date_sub(current_date,1)\n" +
                "                    and is_parent = '0'\n" +
                "                    and business_type in ('T020', 'T022', 'T039')\n" +
                "                    and business_state in ('2050', '2070', '2080')\n" +
                "                    and payment_state = '1'\n" +
                "                ) a\n" +
                "            ) a1 on a1.parent_request_id = t.trade_request_id\n" +
                "            where a1.parent_request_id is null\n" +
                "            and t.dt=date_sub(current_date,1)\n" +
                "            and t.business_type in ('T020', 'T022', 'T039')\n" +
                "            and t.business_state in ('2050', '2070', '2080')\n" +
                "            and t.payment_state = '1'\n" +
                "            and t.is_parent = '1'\n" +
                "        ) a\n" +
                "        group by a.custno\n" +
                "\n" +
                "        union all\n" +
                "\n" +
                "        select  sum(t.confirm_amount) as balance,  t.custno\n" +
                "        from (\n" +
                "            select * from dwd.dwd_ordr_fund_trade_request_full\n" +
                "            where dt=date_sub(current_date,1)\n" +
                "            and business_type = 'T024'\n" +
                "            and business_state = '2090'\n" +
                "        ) t\n" +
                "        left join (\n" +
                "            select a.trade_request_id\n" +
                "            from (\n" +
                "                select * from dwd.dwd_ordr_fund_trade_request_full\n" +
                "                where dt=date_sub(current_date,1)\n" +
                "                and business_type = 'T099'\n" +
                "                and business_state in ('2050', '2070', '2080')\n" +
                "            ) a\n" +
                "            where not exists (\n" +
                "                select 1\n" +
                "                from (\n" +
                "                    select * from dwd.dwd_ordr_fund_trade_request_full\n" +
                "                    where dt=date_sub(current_date,1)\n" +
                "                    and business_type = 'T022'\n" +
                "                ) b\n" +
                "                where b.parent_request_id = a.trade_request_id\n" +
                "            )\n" +
                "        ) t1 on t.parent_request_id = t1.trade_request_id\n" +
                "        where t1.trade_request_id is not null\n" +
                "        group by t.custno\n" +
                "    ) aa on fu.custno = aa.custno\n" +
                "    where aa.balance is not null and aa.balance > 0\n" +
                "    group by fu.user_id\n" +
                "    ) orcl_for2\n" +
                "on orcl_for2.user_id=base.user_id\n" +
                "\n" +
                "left join\n" +
                "    (\n" +
                "    select\n" +
                "\tsum(t.available_amount)+sum(t.return_available_buy_amount)+sum(t.frozen_amount)+sum(t.appoint_buy_amount) as amount, t.user_id  as user_id\n" +
                "    from (select * from ods.fengfd_fc_user_account_full where dt = date_sub(current_date(),1) ) t\n" +
                "    group by t.user_id\n" +
                "    ) orcl_for3\n" +
                "on orcl_for3.user_id=base.user_id\n" +
                "\n" +
                "left join\n" +
                "    (select sum(available_amount) as balance, user_id   from dwd.dwd_user_fund_cash_account_full where dt = date_sub(current_date(),1)  group by user_id ) acc\n" +
                "on acc.user_id=base.user_id\n" +
                "left join\n" +
                "    (select sum(amount) as amount,user_id from  dwd.dwd_ordr_fund_cash_ordr_full t\n" +
                "        where dt = date_sub(current_date(),1) and order_type in ('RECHARGE','EN_RECHARGE','APPOINT_PURCHASE_SECURITIES','STRATEGY_APPOINT_BUY') and ((retcode = '0000' and t.order_status = 'ACCEPT_SUCCESS') or t.order_status = 'CONFIRM_SUCCESS')\n" +
                "     GROUP BY t.user_id ) ord\n" +
                " on ord.user_id=base.user_id\n" +
                " left join\n" +
                "    (SELECT fu.user_id as user_id, aa.balance as bala\n" +
                "    FROM (select * from dwd.dwd_user_fund_user_full where dt = date_sub(current_date(),1)) fu\n" +
                "    LEFT JOIN(\n" +
                "    select sum(t.application_amount) AS balance, t.custno\n" +
                "    from (\n" +
                "        SELECT * from dwd.dwd_ordr_fund_trade_request_full\n" +
                "        where dt=date_sub(current_date,1)\n" +
                "        and is_parent = 0\n" +
                "        and return_channel_id in ('I','K')\n" +
                "        and payment_state = '1'\n" +
                "        and business_state in ('2050', '2070','2080','2090')\n" +
                "        and business_type in ('T020','T022', 'T039')\n" +
                "        and confirm_state <> 0\n" +
                "    ) t\n" +
                "    GROUP BY t.custno\n" +
                "    ) aa\n" +
                "    ON fu.custno = aa.custno) orcl\n" +
                "on base.user_id = orcl.user_id\n" +
                " left join\n" +
                "  (select sum(if(concat(year(confirm_time),month(confirm_time)) = concat(year(current_date()),month(current_date())),received_amount,null)) as amount,user_id from dwi.dwi_ordr_fortune_tb_found_payment_schedule group by user_id )sch\n" +
                "  on base.user_id = sch.user_id\n" +
                " left join\n" +
                "  (select user_id,sum(received_payment_amount) as amount from ods.wealth_sales_tb_overseas_payment_schedule_full\n" +
                "    where dt=date_sub(current_date,1) and concat(year(current_date()),month(current_date()))=concat(year(received_payment_time),month(received_payment_time)) group by user_id)ove1\n" +
                "  on base.user_id=ove1.user_id\n" +
                " left join\n" +
                " (\n" +
                "    select sum(pay_amount) as amount,user_id from dwd.dwd_ordr_fortune_pfund_ordr_full\n" +
                "           where  dt = date_sub(current_date(),1) and `order_status` in ('22','29')  and `fund_match_status`='2' and is_deleted = 0 group by user_id\n" +
                " )pfun\n" +
                " on base.user_id=pfun.user_id";

        Set<String> tableSet = new HashSet<>();
        for (String sql :
                sqls.split(";")) {
            tableSet.addAll(getLineInfo(sql));
        }

        for (String table :tableSet) {
            System.out.println(table);
        }



    }

    public static Set<String> getLineInfo(String query) throws IOException, ParseException,
            SemanticException {


        LineageInfo lep = new LineageInfo();

        lep.getLineageInfo(query);
        HashSet<String> set = new HashSet<>();

        for (Object tab : lep.getInputTableList()) {
//            System.out.println("InputTable=" + tab);
            set.add(tab.toString()
                    .replaceAll("ods\\.", "")
                    .replaceAll("dwd\\.", "")
                    .toLowerCase());

        }

        return set;

//        for (Object tab : lep.getOutputTableList()) {
//            System.out.println("OutputTable=" + tab);
//        }
    }
    
    public static int getJobs(String command) throws Exception{
        //获取hive配置对象
        HiveConf conf = new HiveConf();
//        conf.addResource(new Path("file:///usr/local/apache-hive-0.13.1-bin/conf/hive-site.xml"));
//        conf.addResource(new Path("file:///usr/local/apache-hive-0.13.1-bin/conf/hive-default.xml.template"));
        conf.set("javax.jdo.option.ConnectionURL",
                "jdbc:mysql://10.255.51.23:3306/hive?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8");
        conf.set("hive.metastore.local", "true");
        conf.set("javax.jdo.option.ConnectionDriverName","com.mysql.jdbc.Driver");
        conf.set("javax.jdo.option.ConnectionUserName", "root");
        conf.set("javax.jdo.option.ConnectionPassword", "root");
        conf.set("hive.stats.dbclass", "jdbc:mysql");
        conf.set("hive.stats.jdbcdriver", "com.mysql.jdbc.Driver");
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict");

  
        SessionState.start(conf);
        Context ctx = new Context(conf);

        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(command);

        tree = ParseUtils.findRootNonNullToken(tree);

        BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);

        sem.analyze(tree,ctx);

        sem.validate();
        Schema schema = Driver.getSchema(sem, conf);
        HiveOperation hiveOperation = SessionState.get().getHiveOperation();
        String queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
        QueryPlan queryPlan = new QueryPlan(command,sem,System.currentTimeMillis(),queryId,hiveOperation,schema);
        int jobs = Utilities.getMRTasks(queryPlan.getRootTasks()).size();
    
        System.out.println("Total jobs = " + jobs);
        return jobs;
    }
}
