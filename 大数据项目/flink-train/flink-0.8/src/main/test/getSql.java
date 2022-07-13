/**
 * @ClassName getSql
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-15 15:40
 * @Version 1.0
 */
public class getSql {
    public static void main(String[] args) {
        String sql="INSERT OVERWRITE TABLE dwi.trader_amount_tb_user_asset_infeed PARTITION(dt)\n" +
                "select base.`(create_time|modify_time|dt|balance)?+.+`,fortune.asset_amount,fortune.unrepay_interest,fortune.paid_interest,balance,create_time,modify_time,dt\n" +
                "from\n" +
                "    (select * from dwi.trader_amount_tb_user_asset_infeed_fix where dt= '2020-01-14' ) base\n" +
                "left join\n" +
                "    (\n" +
                "           select base.user_id as user_id,\n" +
                "   cast(nvl(fplan.stay_for,0)+nvl(orl.amount,0)\n" +
                "   --+nvl(pfun.amount,0)\n" +
                "   as decimal(15,2)) as asset_amount,\n" +
                "   cast(0 as decimal(15,2)) as unrepay_interest,\n" +
                "   cast(nvl(forn.totalIncome,0) as decimal(15,2)) as paid_interest\n" +
                "   from\n" +
                "   (select id,user_id,full_name,phone_mobile,register_datetime,overseas_regsiter_datetime,overseas_user_id,\n" +
                "        lock_date,append_channel,bind_sam_id,bind_group_manager_id,bind_city_manager_id from dwd.dwd_fortune_fortune_customers where max_time>'2020-01-14' and eff_time<='2020-01-14' and deleted=0) base\n" +
                "        left join\n" +
                "   (\n" +
                "    SELECT\n" +
                "    a.online_user_id as user_id ,\n" +
                "    --`持有财富金额\n" +
                "    nvl(sum(if(l.LOAN_ID is null,PREDICT_AMOUNTPRINCIPAL , null )),0) as stay_for,\n" +
                "    FROM\n" +
                "      (select * from dwd.dwd_fortune_fortune_repayment_plan where max_time>'2020-01-14' and eff_time<='2020-01-14' ) AS a\n" +
                "    LEFT JOIN (\n" +
                "      SELECT LOAN_ID,PERIOD_ID,CURRENT_PERIOD\n" +
                "      from dwd.dwd_fortune_fortune_repayment_plan_exec_log\n" +
                "        where max_time>'2020-01-14' and eff_time<='2020-01-14' and sub_type = 1\n" +
                "        group by loan_id ,period_id,current_period) AS l\n" +
                "    on a.LOAN_ID = l.LOAN_ID and a.PERIOD_ID = l.PERIOD_ID and a.CURRENT_PERIOD = l.CURRENT_PERIOD\n" +
                "    group by a.online_user_id\n" +
                "    ) fplan\n" +
                "    on fplan.user_id=base.user_id\n" +
                "left join\n" +
                "    --私募持仓资产\n" +
                "    (select sum(t.balance * nvl(c.nav, 1) + nvl(t.PAYABLE_PROFIT, 0)) as amount,u.user_id as user_id\n" +
                "        from (select * from dwd.dwd_fengpe_tr_trust_channel_share where max_time>'2020-01-14' and eff_time<='2020-01-14' and balance > 0 ) t\n" +
                "        left join (\n" +
                "\n" +
                "                   select distinct m.fund_code, f.nav, m.nav_date\n" +
                "                     from (select fund_code, max(t.nav_actual_date) as nav_date\n" +
                "                              from (select * from dwd.dwd_fengpe_cl_fund_market where max_time>'2020-01-14' and eff_time<='2020-01-14') t\n" +
                "                             group by fund_code) m\n" +
                "                     left join (select * from dwd.dwd_fengpe_cl_fund_market where max_time>'2020-01-14' and eff_time<='2020-01-14') f\n" +
                "                       on m.nav_date = f.nav_actual_date\n" +
                "                      and f.fund_code = m.fund_code\n" +
                "\n" +
                "                   ) c\n" +
                "          on t.fund_code = c.fund_code\n" +
                "      left join (select * from dwd.dwd_fengpe_ac_trade_account where max_time>'2020-01-14' and eff_time<='2020-01-14') ac on ac.trade_account_no = t.trade_account_no\n" +
                "      left join (select * from dwd.dwd_fengfd_fd_user where max_time>'2020-01-14' and eff_time<='2020-01-14') u on u.cust_no = ac.customer_no\n" +
                "      group by u.user_id)orl\n" +
                "         on base.user_id = orl.user_id\n" +
                "        left join\n" +
                "        (\n" +
                "        \tSELECT\n" +
                "         nn.ONLINE_USER_ID  user_id,sum(nn.REAL_AMOUNTINTEREST) totalIncome\n" +
                "        FROM\n" +
                "          (select ONLINE_USER_ID,REAL_AMOUNTINTEREST,loan_id from dwd.dwd_fortune_fortune_repayment_plan where max_time>'2020-01-14' and eff_time<='2020-01-14' )nn\n" +
                "          LEFT JOIN\n" +
                "        \t(select id,type from dwd.dwd_fortune_fortune_loan where max_time>'2020-01-14' and eff_time<='2020-01-14' )  ff\n" +
                "        on  nn.loan_id = ff.id\n" +
                "        where ff.type in(0,6)\n" +
                "        GROUP BY nn.ONLINE_USER_ID) forn\n" +
                "        on base.user_id = forn.user_id\n" +
                "        where length(base.user_id) > 0\n" +
                "    )fortune\n" +
                "    on base.user_id = fortune.user_id;\n";
    }

}
