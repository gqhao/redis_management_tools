#!/usr/bin/python27
#encoding:utf8
import paramiko
import redis
import socket
import sys
from time import sleep
import time
import MySQLdb as my
import pymssql as py
import io

user_serveradmin=''
passwd_serveradmin=''

########返回当前时间
def getdate():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

#######创建数据库连接
def get_database_connections(host,port,db,user,password,dbtype):
    if dbtype.lower()=='mysql':
        try:
            myconn=my.connect(host=host,port=port,db=db,user=user,passwd=password,charset='utf8')
        except Exception,e:
            print Exception,e
            return -1,''
        return 1,myconn
    elif dbtype.lower()=='sqlserver':
        try:
            mssqlconn=py.connect(host=host,port=port,database=db,user=user,password=password,charset='utf8')
        except Exception,e:
            print Exception,e
            return -1, ''
        return 1,mssqlconn
    else:
        return -1,''

##check the whether the server is available
def checkopen(dns, port):
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.settimeout(2)
    check_times=5
    check_count=0
    check_result=1
    while True:
        if check_count>=check_times:
            break
        try:
            sk.connect((dns, int(port)))
        except:
            check_result=0
            sleep(1)
        check_count+=1
        if check_result==1:
            break
    sk.close()
    return check_result

#####get the slave(s) node information (ip address )
def get_slave_nodes(keyslist):
    slave_list=[]
    keys=keyslist.keys()
    for item in keys:
        if item.find('slave')==0:
            slave_list.append(keyslist[item])
    return slave_list
###get the human-friendly key-size
def get_Human_Size(value):
    gb = 1024 * 1024 * 1024.0
    mb = 1024 * 1024.0
    kb = 1024.0
    if value >= gb:
        return str(round(value / gb, 2)) + " gb"
    elif value >= mb:
        return str(round(value / mb, 2)) + " mb"
    elif value >= kb:
        return str(round(value / kb, 2)) + " kb"
    else:
        return str(value) + "b"

######get key list
def get_key_list(reconn):
    keyslist=[]
    tmp_keyslist=reconn.scan(0)
    for item in tmp_keyslist[1]:
        keyslist.append(item)
    while True:
        if tmp_keyslist[0]==0:
            break
        tmp_keyslist=reconn.scan(tmp_keyslist[0])
        for item in tmp_keyslist[1]:
            keyslist.append(item)
    return keyslist
######## get zset member#########
def get_zset_member(reconn,zset_keyname):
    member_list=[]
    tmp_member_list=reconn.zscan(zset_keyname,0)
    for item in tmp_member_list[1]:
        member_list.append(item[0])
    while True:
        if tmp_member_list[0]==0:
            break
        tmp_member_list=reconn.zscan(zset_keyname,tmp_member_list[0])
        sleep(0.005)
        for item in tmp_member_list[1]:
            member_list.append(item[0])
    return member_list
def get_nottl_keys(reconn,keyslist):
    redis_pipe=reconn.pipeline()
    commit_flag=0
    check_result=[]
    nottl_keyslist=[]
    for item in keyslist:
        if commit_flag==200:
            commit_flag=0
            tt_result=redis_pipe.execute()
            for tt_item in tt_result:
                check_result.append(tt_item)
        redis_pipe.ttl(item)
        commit_flag+=1
    tt_result = redis_pipe.execute()
    for tt_item in tt_result:
        check_result.append(tt_item)
    for i in range(len(check_result)):
        if not check_result[i]:
            nottl_keyslist.append(keyslist[i])
    return nottl_keyslist

###### get hash fields################
def get_hash_field(reconn,hash_keyname):
    field_list = []
    tmp_field_list = reconn.hscan(hash_keyname, 0)
    for item in tmp_field_list[1].keys():
        field_list.append(item)
    while True:
        if tmp_field_list[0] == 0:
            break
        tmp_field_list = reconn.hscan(hash_keyname, tmp_field_list[0])
        sleep(0.005)
        for item in tmp_field_list[1].keys():
            field_list.append(item)
    return field_list
########get the os available memory
def get_availabe_memroy_os(host):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, 22, username='', password='')
        stdin, stdout, stderr = ssh.exec_command("cat /proc/meminfo |grep MemFree")
        output = stdout.readline()
        memory_free = int(output.replace('\n', '').replace('MemFree', '').replace(' ', '').replace(':', '').replace('kB', ''))
        ssh.close()
        if not output:
            return -1
        return memory_free
    except Exception as e:
        return -1

########get the os available memory
def get_availabe_memroy_os_debug(host):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, 1022, username='',timeout=1, password='')
        stdin, stdout, stderr = ssh.exec_command("cat /proc/meminfo |grep MemFree")
        output = stdout.readline()
        memory_free = int(
            output.replace('\n', '').replace('MemFree', '').replace(' ', '').replace(':', '').replace('kB', ''))
        ssh.close()
        if not output:
           return -1
        return memory_free
    except Exception as e:
        return -1

######get key type
def get_key_type(reconn,keyname):
    return reconn.type(keyname)

def delete_key(reconn_slave,reconn,keyname,task_id,createuser):
    try:
        reconn_local=redis.Redis(host='127.0.0.1',port=6379)
        #reconn_local.hset('safely_delete_key', 'emergency_stop',0)
    except Exception,e:
        print Exception,e
    finished_key_number=0
    redis_pipe=reconn.pipeline()
    key_type=get_key_type(reconn,keyname)
    file_str=open('/tmp/string_type_list.txt','a')
    if key_type=='string':                       ######delete keys which the key type is string
        #print '########', keyname, ' is a string key,I will call delete to delete the key if the key size is lower than 1GB'
        #key_size=reconn.credis_debug_object(keyname)['serializedlength']
        #if key_size>107374182:
        #    print 'The string key is bigger than 100 MB,please delete it manually'
        #    return (-1,'The string key is bigger than 100 MB,please delete it manually')
        #try:
        #    reconn.delete(keyname)
        #except Exception,e:
        #    print Exception,e
        #    return (-1,'Something wrong has happend,please check manually!')
        #return(1,'The key '+keyname+' has been deleted successfully!')
        str_len=reconn.strlen(keyname)
        if str_len>100000000:
            print '##########################################################################'
            print keyname,' is string type,and bigger than 100M,please delete manually!'
            sleep(10)
            return (-1, keyname+' is string type,and bigger than 100M,please delete manually!')
        else:
            reconn.delete(keyname)
        return (1, 'string type,keep keyanem in log file')
    elif key_type=='list':                        ######delete keys which the key type is list
        #print '########', keyname, ' is a list key,I will call lpop to delete content,then deleting the key'
        commit_flag=0
        write_log_flag=0
        key_len=reconn.llen(keyname)
        while True:
            if write_log_flag>=10000:
                write_log_flag=0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                print keyname,u'已完成',str(finished_key_number) + '/' + str(key_len)+'\n'
            current_llen=reconn.llen(keyname)
            if not current_llen:
                reconn.delete(keyname)
               # print 'The key '+keyname+' has been deleted successfully!'
                return (1,'The key '+keyname+' has been deleted successfully!')
            if commit_flag==200:
                print 'keylist_length:',reconn.llen(keyname)
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    print 'Emergency stop,please check manually!'
                    file_str.write('key type list,emergency quit:|' + keyname + '\n')
                    del reconn
                    del reconn_slave
                    return (-1, 'Emergency stop,please check manually!')
                tt=redis_pipe.execute()
                del tt
                commit_flag=0
                sleep(0.1)
            if current_llen<200:
                #print 'keylist_length:', reconn.llen(keyname)
                tt=redis_pipe.execute()
                del tt
            redis_pipe.lpop(keyname)
            commit_flag+=1
            finished_key_number +=1
            write_log_flag +=1
    elif key_type=='set':                         ######delete keys which the key type is set
        #print '########', keyname, ' is a set key,I will call spop to delete members,then deleting the key'
        commit_flag=0
        write_log_flag=0
        key_len=reconn.scard(keyname)
        while True:
            if write_log_flag>10000:
                write_log_flag=0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                print keyname,u'已完成',str(finished_key_number)+'/'+str(key_len)+'\n'
            current_llen = reconn.scard(keyname)
            if not current_llen:
                reconn.delete(keyname)
                #print 'The key ' + keyname + ' has been deleted successfully!'
                return (1, 'The key ' + keyname + ' has been deleted successfully!')
            if commit_flag==200:
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    print 'Emergency stop,please check manually!'
                    file_str.write('key type set,emergency quit:|' + keyname + '\n')
                    return (-1, 'Emergency stop,please check manually!')
                #print 'set commit'
                redis_pipe.execute()
                commit_flag=0
                sleep(0.1)
            if current_llen<200:
                redis_pipe.spop(keyname)
            redis_pipe.spop(keyname)
            commit_flag+=1
            write_log_flag+=1
            finished_key_number+=1
    elif key_type=='zset':                         ######delete keys which the key type is zset
        #print '########', keyname, ' is a zset key,I will call zrem to delete fileds,then deleting the key'
        commit_flag=0
        write_log_flag=0
        print '######## Getting zset members!'
        print u'获取zset类型key的members'
        member_list=get_zset_member(reconn_slave,keyname)
        #print '######## zset members getted! Now start zset key deleting!'
        key_len=len(member_list)
        for item in member_list:
            if write_log_flag>10000:
                write_log_flag=0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                print keyname,u'已完成',str(finished_key_number) +'/'+str(key_len)+'\n'
            if commit_flag==200:
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    print 'Emergency stop,please check manually!'
                    file_str.write('key type zset,emergency quit:|' + keyname + '\n')
                    del reconn
                    del reconn_slave
                    return (-1, 'Emergency stop,please check manually!')
                redis_pipe.execute()
                commit_flag=0
                sleep(0.1)
            redis_pipe.zrem(keyname,item)
            commit_flag+=1
            write_log_flag+=1
            finished_key_number+=1
        redis_pipe.execute()
        del member_list
        if finished_key_number > key_len:
            finished_key_number = key_len
            print keyname,u'已完成',str(finished_key_number) + '/' + str(key_len)
        reconn.delete(keyname)
        #print 'The key ' + keyname + ' has been deleted successfully!'
        return (1, 'The key ' + keyname + ' has been deleted successfully!')
    elif key_type=='hash':                         ######delete keys which the key type is hash
        print '########',keyname,' is a hash key,I will call hdel to delete fileds,then deleting the key'
        commit_flag=0
        write_log_flag=0
        finished_key_number=0
        #print '######## Getting hash fileds!'
        print '获取hash类型key的fields\n'
        field_list=get_hash_field(reconn_slave,keyname)
        print 'fields 已获取，开始删除\n'
        #print '######## hash fields getted! Now start hash key deleting!'
        key_len=len(field_list)
        for item in field_list:
            if write_log_flag>10000:
                if finished_key_number>key_len:
                    finished_key_number=key_len
                print keyname,u'已完成',str(finished_key_number)+'/' +str(key_len)+'\n'
                write_log_flag=0
            if commit_flag==200:
                sleep(0.1)
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    print 'Emergency stop,please check manually!'
                    file_str.write('key type hash,emergency quit:|' + keyname + '\n')
                    return (-1, 'Emergency stop,please check manually!')
                #print 'hash commit!'
                redis_pipe.execute()
                commit_flag=0
            redis_pipe.hdel(keyname,item)
            commit_flag+=1
            write_log_flag+=1
            finished_key_number+=1
        redis_pipe.execute()
        del field_list
        if finished_key_number > key_len:
            finished_key_number = key_len
        print keyname,u'已完成',  str(finished_key_number) + '/' + str(key_len)
        reconn.delete(keyname)
        redis_pipe.execute()
        #print 'hash keys deleted,I will retrun the result'
        return (1, 'The key ' + keyname + ' has been deleted successfully!')
    else:
        print keyname,'has notype,please check manually!'
        file_str.write('key type notype,error quit:|' + keyname + '\n')
        return (-1,keyname+' has notype,please check manually!')
def sub_main(start_num):
    current_date=getdate()
    redis_host='10.28.69.105'
    redis_port=6381
    print getdate(),'loading key list\n'
    keyname_file = open('/var/www/html/data/ibu_market_data_nottl.txt')
    print getdate(),'key list loaded!'
    keynames=keyname_file.readlines()
    keyname_counts=len(keynames)
    deleted_count=start_num
    for keyname in keynames[start_num:]:
        if deleted_count%1000==0:
            print current_date,'Start deleting '+str(deleted_count)+'/'+str(keyname_counts)+':'+keyname+'\n'
        try:
            reconn_romote=redis.Redis(host=redis_host,port=int(redis_port))
        except Exception,e:
            print Exception,e
            print current_date, u'无法连接redis服务器,redis_host:',redis_host,',redis_port:',redis_port
  #########get information of slave nodes
        slave_list=get_slave_nodes(reconn_romote.info('replication'))
        #if not slave_list:
        slave_ip=redis_host
        slave_port=redis_port
        #else:
        #    slave_ip=slave_list[-1]['ip']
        #    slave_port=slave_list[-1]['port']
        try:
            reconn_remote_slave=redis.Redis(host=slave_ip,port=int(slave_port))
        except Exception,e:
            print Exception,e
#### ######## check weither delete one key of many keys.
        keyname=keyname.replace('\n','')
        #keyname=keyname.decode('gbk')
        isexist=reconn_remote_slave.exists(keyname)
        if not isexist:
            print keyname,'doesnot exists,plsease check manually!'
        else:
            del_result=delete_key(reconn_remote_slave,reconn_romote,keyname,1,'gqhao')
            if del_result[0]!=1:
                print 'Something wrong has happened,please check manually'
        del reconn_remote_slave
        del reconn_romote
        deleted_count+=1
def main():
    while True:
        try:
            sub_main()
        except Exception,e:
            print Exception,e
        sleep(5)
if __name__ == '__main__':
    main()