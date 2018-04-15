#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import redis
import MySQLdb as my
from rat_api import *
import multiprocessing as mp
import sys
import time
user_serveradmin=''
passwd_serveradmin=''
######获取sentinel 目前监控的所有group
def get_sentinel_monitor_group_name(myconn):
  redis_groups = []
  cur_myconn=myconn.cursor()
  cur_myconn.execute("select idc,group_id,max(ip),port from redis_sentinel_list_new group by idc,group_id,port;")
  tt=cur_myconn.fetchall()
  for item in tt:
    re_conn = redis.Redis(host=item[2], port=item[3])
    redis_info = re_conn.sentinel_masters()
    group_name = redis_info.keys()
    for item1 in group_name:
      redis_groups.append((item[0], item1.lower()))
  if redis_groups:
    cur_myconn.close()
    del cur_myconn
    return((True,redis_groups))
  cur_myconn.close()
  del cur_myconn
  return((False,'Cannot get group information from sentinels,please check manually!'))

##获取指定group的idc code
def get_idc_group_name(thread_id,tt,return_dict):
  redis_groups=[]
  for item in tt:
    idc_code=get_idc_code(item[2])
    if idc_code[0]:
      redis_groups.append((True,idc_code[1].lower(),item[1]))
    else:
      redis_groups.append((False,'Cannot get idc code from Redis-Rat',item[1]))
    return_dict[thread_id]=redis_groups

###group较多，因此开启5个并发进程获取idc code
def get_redis_group_name():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  redis_groups=[]
  process_num=5
  manager = mp.Manager()
  return_dict = manager.dict()
  jobs = []
  cur_myconn=myconn.cursor()
  cur_myconn.execute("select distinct a.name as clustername,c.name as groupname,b.ip as host,b.port from redisadmin_rediscluster a join redisadmin_redisinstance b on a.pool_id=b.pool_id join redisadmin_redisgroup c on a.pool_id=c.pool_id  and b.group_id=c.group_id where b.parentid=0 and a.status=1 and c.name not in (select groupname from redis_group_check_whitelist);")
  tt=cur_myconn.fetchall()
  cur_myconn.close()
  myconn.close()
  slice_count=len(tt)/process_num
  for i in range(process_num):
    if i ==process_num-1:
      p=mp.Process(target=get_idc_group_name, args=(i,tt[i*slice_count:],return_dict))
    else:
      p=mp.Process(target=get_idc_group_name, args=(i,tt[i*slice_count:(i+1)*slice_count],return_dict))
    jobs.append(p)
    p.start()
  for process in jobs:
    process.join()
  for key in return_dict.keys():
    redis_group=return_dict[key]
    for item in redis_group:
      redis_groups.append(item)
  return redis_groups

def problom_trace(myconn,groupinfo):
  idc=groupinfo[0]
  groupname=groupinfo[1]
  cur_myconn=myconn.cursor()
  cur_myconn.execute("select name from redisadmin_redisgroup where name='"+groupname+"';")
  tt=cur_myconn.fetchall()
  if not tt:
    cur_myconn.close()
    return(('Group does not exists',{'idc':idc,'groupname':groupname}))
  cur_myconn.execute("select count(1) from redisadmin_redisgroup a join redisadmin_redisinstance b on a.pool_id=b.pool_id and a.group_id=b.group_id and b.status=1 and b.parentid=0 and a.name='"+groupname+"'")
  tt=cur_myconn.fetchone()
  if tt[0]==0:
    cur_myconn.close()
    return (('Group or instance is not active', {'idc':idc,'groupname':groupname} ))
  if tt[0]==1:
    cur_myconn.execute("select count(1) from sentinel_group_name_tmp where group_name='"+groupname+"'")
    tt_sentinel=cur_myconn.fetchone()
    if tt_sentinel[0]==1:
      cur_myconn.close()
      return(('Master and Sentinel are not in the same IDCs',{'idc':idc,'groupname':groupname}))
    else:
      cur_myconn.close()
      return(('Has sentienls in OYIDC and JQIDC',{'idc':idc,'groupname':groupname}))
  cur_myconn.close()
  return (('Other Error',{'idc':idc,'groupname':groupname}))

####检查出每秒访问次数低于10的Group
def group_few_ops():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn=myconn.cursor()
  cur_myconn.execute("select clustername,name as groupname,case when group_ops<0 then 0 else group_ops end as group_ops from (select c.name as clustername,a.name,sum(instance_ops) as group_ops  from redisadmin_redisgroup a join redisadmin_redisinstance b on a.pool_id=b.pool_id and a.group_id=b.group_id join redisadmin_rediscluster c on a.pool_id=c.pool_id where a.status=1 and b.can_read=1 and c.create_time<date_add(now(),interval -1 MONTH) and  c.status =1 group by c.name,a.name) aa  where aa.name not in (select groupname from redis_group_check_whitelist) and group_ops< 100 order by group_ops;")
  tt=cur_myconn.fetchall()
  cur_myconn.close()
  myconn.close()
  group_few_ops_result=[]
  if tt:
    for item in tt:
      group_few_ops_result.append({'clustername':item[0],'groupname':item[1],'opspersec':item[2]})
  return group_few_ops_result

def sentinel_group_check():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn=myconn.cursor()
  err_groups=[]
  sentinel_groups_tmp=get_sentinel_monitor_group_name(myconn)
  if not sentinel_groups_tmp[0]:
    return sentinel_groups_tmp
  sentinel_groups=sentinel_groups_tmp[1]
  prd_groups_tmp=get_redis_group_name()
  prd_groups=[]
  for item in prd_groups_tmp:
    if item[0]:
      prd_groups.append((item[1],item[2]))
    else:
      err_groups.append((item[2],item[1]))
  cur_myconn.execute("drop table if exists sentinel_group_name_tmp;")
  cur_myconn.execute("create table sentinel_group_name_tmp(id int auto_increment primary key not null,idc_code varchar(255),group_name varchar(255));")
  cur_myconn.execute("create index idx_idccode_groupname on sentinel_group_name_tmp(idc_code,group_name)")
  cur_myconn.execute("drop table if exists prd_group_name_tmp;")
  cur_myconn.execute("create table prd_group_name_tmp(id int auto_increment primary key not null,idc_code varchar(255),group_name varchar(255));")
  cur_myconn.execute("create index idx_idccode_groupname on prd_group_name_tmp(idc_code,group_name)")
  slice_count=len(prd_groups)/5
  for i in range(5):
    if i ==4:
      try:
        cur_myconn.executemany("insert into sentinel_group_name_tmp(idc_code,group_name) values(%s,%s)",sentinel_groups[i*slice_count:])
        cur_myconn.executemany("insert into prd_group_name_tmp(idc_code,group_name) values(%s,%s)",prd_groups[i * slice_count:])
        myconn.commit()
      except  Exception,e:
        print Exception,e
    else:
      try:
        cur_myconn.executemany("insert into sentinel_group_name_tmp(idc_code,group_name) values(%s,%s)",sentinel_groups[i*slice_count:(i+1)*slice_count])
        cur_myconn.executemany("insert into prd_group_name_tmp(idc_code,group_name) values(%s,%s)",prd_groups[i * slice_count:(i+1)*slice_count])
        myconn.commit()
      except Exception,e:
        print Exception,e
#### 检测未添加sentinel 的group
  cur_myconn.execute("select distinct  a.idc_code,a.group_name from prd_group_name_tmp a left join sentinel_group_name_tmp b on a.idc_code=b.idc_code and a.group_name=b.group_name where b.idc_code is null and a.group_name not in (select groupname from redis_group_check_whitelist)order by a.idc_code,a.group_name;")
  tt=cur_myconn.fetchall()
  groups_no_sentinel=[]
  for item in tt:
    groups_no_sentinel.append({'idc':item[0],'groupname':item[1]})
  del tt
####检测sentinel添加不合规
  cur_myconn.execute("select distinct  a.idc_code,a.group_name from sentinel_group_name_tmp a left join prd_group_name_tmp b on a.idc_code=b.idc_code and a.group_name=b.group_name where b.idc_code is null  and a.group_name not  in (select groupname from redis_group_check_whitelist) order by a.idc_code,a.group_name;")
  tt=cur_myconn.fetchall()
  sentinel_error_groups=[]
  for item in tt:
    error_info_tmp=problom_trace(myconn,item)
    sentinel_error_groups.append(error_info_tmp)
  cur_myconn.close()
  myconn.close()
  return ((sentinel_error_groups,groups_no_sentinel))

def group_no_master():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn=myconn.cursor()
  cur_myconn.execute("SELECT distinct aa.clustername,aa.groupname FROM (SELECT DISTINCT a.name AS clustername ,c.name AS groupname FROM    redisadmin_rediscluster a JOIN redisadmin_redisinstance b ON a.pool_id = b.pool_id JOIN redisadmin_redisgroup c ON a.pool_id = c.pool_id AND b.group_id = c.group_id WHERE b.parentid=0 and a.status = 1 and c.name not in (select groupname from redis_group_check_whitelist)) aa  left JOIN (SELECT DISTINCT a.name AS clustername , c.name AS groupname FROM    redisadmin_rediscluster a JOIN redisadmin_redisinstance b ON a.pool_id = b.pool_id   JOIN redisadmin_redisgroup c ON a.pool_id = c.pool_id WHERE   b.parentid = 0 AND a.status = 1 AND b.status=1) bb ON aa.clustername=bb.clustername AND aa.groupname=bb.groupname where bb.groupname is null order by aa.clustername,aa.groupname")
  tt=cur_myconn.fetchall()
  cur_myconn.close()
  myconn.close()
  result=[]
  for item in tt:
    result.append({'clustername':item[0],'groupname':item[1]})
  if tt:
    return ((True,result))
  else:
    return((False,'All groups has master'))

####检测主从是否在同一IDC，主从版本是否一致
def group_master_slave_diffidc_instance(thread_id,group_list,return_dict):
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn=myconn.cursor()
  error_groups=[]
  for item in group_list:
    cur_myconn.execute("select distinct ip,redis_version from redisadmin_redisinstance where pool_id= "+str(item[0])+" and group_id="+str(item[1]))
    tt=cur_myconn.fetchall()
    idc_code1 = get_idc_code(tt[0][0])
    idc_code2=get_idc_code(tt[1][0])
    if not idc_code1[0] or not idc_code2[0]:
      continue
    if idc_code1[1].lower()!=idc_code2[1].lower():
      #print 'thread_id',thread_id,item
      error_groups.append(('Different IDCs',{'groupname':item[2]}))
    if tt[0][1]!=tt[1][1]:
      #print 'thread_id', thread_id, item
      #print 'thread_id',tt
      error_groups.append(('Different Versions',{'groupname':item[2]}))
  cur_myconn.close()
  myconn.close()
  return_dict[thread_id]=error_groups

def groups_master_slave_dffidc():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn=myconn.cursor()
  error_groups=[]
  process_num = 5
  manager = mp.Manager()
  return_dict = manager.dict()
  jobs = []
  cur_myconn.execute("select  a.pool_id,a.group_id,a.name as groupname,count(1) from redisadmin_redisgroup a join redisadmin_redisinstance b on a.pool_id=b.pool_id and a.group_id=b.group_id and b.status=1 where a.name not in(select groupname from redis_group_check_whitelist) group by a.pool_id,a.group_id,a.name having(count(1))=2;")
  tt=cur_myconn.fetchall()
  cur_myconn.close()
  myconn.close()
  slice_count = len(tt) / process_num
  for i in range(process_num):
    if i ==process_num-1:
      p=mp.Process(target=group_master_slave_diffidc_instance, args=(i,tt[i*slice_count:],return_dict))
    else:
      p=mp.Process(target=group_master_slave_diffidc_instance, args=(i,tt[i*slice_count:(i+1)*slice_count],return_dict))
    jobs.append(p)
    p.start()
  for process in jobs:
    process.join()
  for key in return_dict.keys():
    redis_group = return_dict[key]
    for item in redis_group:
      error_groups.append(item)
  return error_groups

def group_no_ha_check():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn = myconn.cursor()
  cur_myconn.execute("select a.name as groupname from redisadmin_redisgroup a join (select group_id,count(1) from redisadmin_redisinstance group by group_id having(count(1)=1)) b on a.group_id=b.group_id where a.name not in (select groupname from redis_group_check_whitelist) order by a.name")
  tt=cur_myconn.fetchall()
  if not tt:
    cur_myconn.close()
    myconn.close()
    return False,'No groups without HA'
  error_result=[]
  for item in tt:
    error_result.append({'groupname':item[0]})
  cur_myconn.close()
  myconn.close()
  return  True,error_result

def insert_data_mysql(insert_sql,tt):
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn = myconn.cursor()
  slice_cont=len(tt)/1000
  for i in range(slice_cont):
    if i==slice_cont-1:
      try:
        cur_myconn.executemany(insert_sql,tt[i*slice_cont:])
        myconn.commit()
      except Exception,e:
       print Exception,e
    else:
      try:
        cur_myconn.executemany(insert_sql, tt[i * slice_cont:(i+1)*slice_cont])
        myconn.commit()
      except Exception, e:
        print Exception, e

def sub_check_main():
  return_error_msg=[]
  ####检测主从是否在相同的IDC
  sentinel_group_check_result=sentinel_group_check()
  master_slave_check = groups_master_slave_dffidc()
  group_few_ops_result=group_few_ops()
  for item in master_slave_check:
    return_error_msg.append((item[0],'',item[1]['groupname']))
  for item in sentinel_group_check_result[0]:
    return_error_msg.append((item[0],item[1]['idc'],item[1]['groupname']))

  for item in sentinel_group_check_result[1]:
    return_error_msg.append(('Group no sentinel',item['idc'],item['groupname']))
  group_no_master_tmp=group_no_master()
  if group_no_master_tmp[0]:
    for item in group_no_master_tmp:
      return_error_msg.append(('Group no master','',item['groupname']))
  group_no_ha=group_no_ha_check()
  if group_no_ha[0]:
    for item in group_no_ha[1]:
      return_error_msg.append(('Group no HA', '', item['groupname']))
  sql_insert_return_err_msg="insert into redis_group_check_result(problem_desc,idc,groupname) values(%s,%s,%s)"
  insert_data_mysql(sql_insert_return_err_msg,return_error_msg)
  sql_insert_return_ops="insert into redis_group_check_result_ops(clustername,groupname,opspersec) values(%s,%s,%s)"
  return_ops_msg=[]
  for item in group_few_ops_result:
    return_ops_msg.append((item['clustername'],item['groupname'],item['opspersec']))
  insert_data_mysql(sql_insert_return_ops, return_ops_msg)

def check_main():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn = myconn.cursor()
  cur_myconn.execute('select * from redis_group_check_result_ops where datachange_lasttime>date_add(now(),interval -10 minute) limit 1')
  tt=cur_myconn.fetchone()
  cur_myconn.close()
  myconn.close()
  if not tt:
    sub_check_main()
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn = myconn.cursor()
  master_slave_diff_idc = []
  master_slave_diff_version = []
  has_sentinels_in_both_idcs = []
  master_sentinel_diff_idcs = []
  group_doesnot_exist = []
  group_no_sentinel = []
  group_no_master=[]
  group_no_ha = []
  other_error = []
  return_error_msg = {}
  cur_myconn.execute("select dim_name from dim_redis_group_check;")
  tt=cur_myconn.fetchall()
#### 根据问题描述进行故障问题分类
  for item in tt:
    cur_myconn.execute("select idc,groupname from redis_group_check_result where datachange_lasttime>date_add(now(),interval -10 minute) and problem_desc='"+item[0]+"' ")
    tt_result=cur_myconn.fetchall()
    if not tt_result:
      continue
    if item[0].lower()=='different idcs':
      for item_result in tt_result:
        master_slave_diff_idc.append({'groupname':item_result[1]})
    elif item[0].lower()=='different versions':
      for item_result in tt_result:
        master_slave_diff_version.append({'groupname':item_result[1]})
    elif item[0].lower()=='group no sentinel':
      for item_result in tt_result:
        group_no_sentinel.append({'idc':item_result[0],'groupname':item_result[1]})
    elif item[0].lower()=='group no master':
      for item_result in tt_result:
        group_no_master.append({'groupname':item_result[1]})
    elif item[0].lower()=='group no ha':
      for item_result in tt_result:
        group_no_ha.append({'groupname':item_result[1]})
    elif item[0].lower()=='has sentienls in oyidc and jqidc':
      for item_result in tt_result:
        has_sentinels_in_both_idcs.append({'idc':item_result[0],'groupname':item_result[1]})
    elif item[0].lower()=='master and sentinel are not in the same idcs':
      for item_result in tt_result:
        master_sentinel_diff_idcs.append({'idc':item_result[0],'groupname':item_result[1]})
    elif item[0].lower()=='group does not exists':
      for item_result in tt_result:
        group_doesnot_exist.append({'idc':item_result[0],'groupname':item_result[1]})
    else:
      for item_result in tt_result:
        other_error.append({'idc':item_result[0],'groupname':item_result[1]})
  return_error_msg['master_slave_diff_idc'] = master_slave_diff_idc
  return_error_msg['master_slave_diff_version'] = master_slave_diff_version
  return_error_msg['has_sentinels_in_both_idcs'] = has_sentinels_in_both_idcs
  return_error_msg['master_sentinel_diff_idcs'] = master_sentinel_diff_idcs
  return_error_msg['group_doesnot_exist'] = group_doesnot_exist
  return_error_msg['other_error'] = other_error
  return_error_msg['group_no_sentinel'] = group_no_sentinel
  return_error_msg['group_no_master'] = group_no_master
  return_error_msg['group_no_ha'] = group_no_ha
  cur_myconn.execute("select aa.* from (select clustername,round(avg(opspersec)) as avg_ops from redis_group_check_result_ops where datachange_lasttime>date_add(now(),interval -24 hour) group by clustername ) aa  where aa.avg_ops<10 order by aa.clustername;")
  tt=cur_myconn.fetchall()
  group_few_ops_result=[]
  for item in tt:
    group_few_ops_result.append({'clustername':item[0],'ops_per_sec':item[1]})
  return_error_msg['group_few_ops']=group_few_ops_result
  return return_error_msg

def send_email():
  try:
    myconn = my.connect(host='', port=, db='', user=user_serveradmin,
                        passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn = myconn.cursor()
  cur_myconn.execute("select dim_name,max(dim_name_desc) as err_desc, case count(1) when 1 then 0 else count(1)  end as err_count  from dim_redis_group_check a left join redis_group_check_result b on a.dim_name=b.problem_desc and b.datachange_lasttime >date_add(now(),interval -10 minute) group by a.dim_name  union all select 'OpsPerSec','群集每秒访问量低于10次',(select count(1) from (select clustername,avg(opspersec) as avg_ops from redis_group_check_result_ops c where c.datachange_lasttime >date_add(now(),interval -24 hour) group by clustername) aa where aa.avg_ops<10);")
  tt=cur_myconn.fetchall()
  cur_myconn.close()
  myconn.close()
  info_msg = ""
  info_msg += "<!DOCTYPE html><html>\n"
  info_msg+="<aa ><b>Redis集群配置统一检查 </b><a href='http://rat.ops.ctripcorp.com/redisadmin/checkgroup/' target='_blank'>明细</a><Br /><b>汇总如下:</b></aa><table class=MsoNormalTable border=1 cellspacing=0 cellpadding=0 style='font-size:10.0pt;border-collapse:collapse;mso-yfti-tbllook:1184'><tr align=center style='font-weight:bold;mso-yfti-irow:0;mso-yfti-firstrow:yes;background:#DEDEDE;'><td>序号</td><td>检查项</td><td>说明</td><td>异常个数</td></tr>"
  for i in range(len(tt)):
    info_msg += "<tr><td width=40>" + str(i + 1) + "</td><td width=400>" + tt[i][0] + "</td><td width=400>"+tt[i][1]+ "</td><td width=80>"+str(tt[i][2])+"</td>\n"
  info_msg += "</table>\n"
  try:
    myconn = my.connect(host='', port=, db='',
                        user=user_serveradmin, passwd=passwd_serveradmin, charset='utf8')
  except Exception, e:
    print Exception, e
    sys.exist(0)
  cur_myconn=myconn.cursor()
  email_addr = ''
  email_addr+=''
#  cur_myconn.execute(
#    "insert into alert_data_alertsendwait(send_typeid,send_emaillist,msg_subject,msg_email)values(%s,%s,%s,%s)",
#    (1, email_addr, u'Redis 集群配置巡检', info_msg))
#  myconn.commit()
  print info_msg
  cur_myconn.close()
  myconn.close()

def main():
#  print '###########################################'
  try:
    tt=check_main()
    localtime = time.localtime(time.time())
    timehour=localtime.tm_hour
    if timehour==10:
      send_email()
  except Exception,e:
    print Exception,e
if __name__=='__main__':
  main()
