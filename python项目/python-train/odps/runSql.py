import datetime

start_date = datetime.datetime.strptime('20180601','%Y%m%d')

end_date = datetime.datetime.strptime('20200630','%Y%m%d')

middle_date = start_date

# archives_dwd_log_h5_operation_di
# INSERT OVERWRITE TABLE archives_dwd_log_h5_operation_di PARTITION(pt_p) SELECT  log_source,log_time,log_topic,view_name,click_name,cookie,id,time2,ua,openid,begin_time,end_time,time_difference,data2,ext,ext1,log_extract_others,client_type,unionid,gender,ip,from1,type,os_v,content_id,fake_device_id,pt_p  as pt_p    FROM    dpdefault_68367.dwd_log_h5_operation_di where pt_p ='$partitionSpacDate'  CLUSTER  by (if(IS_DIGIT(log_time),log_time,0) );
# archives_dwd_log_h5_operation_di
# INSERT OVERWRITE TABLE archives_dwd_log_h5_operation_di PARTITION(pt_p) SELECT  log_source,log_time,log_topic,view_name,click_name,cookie,id,time2,ua,openid,begin_time,end_time,time_difference,data2,ext,ext1,log_extract_others,client_type,unionid,gender,ip,from1,type,os_v,content_id,null as fake_device_id,pt  as pt_p   FROM    dpdefault_68367.archives_dw_h5_operation where pt ='$partitionSpacDate' CLUSTER  by (if(IS_DIGIT(log_time),log_time,0) );


while(middle_date <= end_date):
    middle_date = middle_date + datetime.timedelta(days=1)
    # 日期
    middle_str = str(middle_date).replace("-",'')[0:8]
    str_sql = '''
        INSERT OVERWRITE TABLE archives_dwd_log_client_user_operation_di PARTITION(pt_p) SELECT  log_source as server_ip   ,if(IS_DIGIT(log_time),log_time,0)  ,log_topic ,action ,substring_index(action,'-',1) as first_path ,substring_index(action,'-',-1) as last_path ,article_id ,SUBSTR(if(IS_DIGIT(create_time),create_time,0),1,18) ,install_from ,ip ,mpua ,mpuuid ,operation ,req_content ,response_code ,sub_version ,time ,ua ,substr(if(IS_DIGIT(user_id),user_id,0),1,18) ,version ,resp  , '-1' as resp_duration , '-1' as log_extract_others ,substr(if(IS_DIGIT(client_type),client_type,0),1,18) ,controller  ,pt  as pt_p  FROM    dpdefault_68367.archives_dwd_log_client_user_operation_di_0_20200630 where pt ='$partitionSpacDate' CLUSTER  by (if(IS_DIGIT(log_time),log_time,0) );
    '''
    print(str_sql.replace('$partitionSpacDate',middle_str))



