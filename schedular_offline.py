########################################## Task Schedular Event ###############################################

#<-------------------------------------------- Imports -------------------------------------------------------->
import mysql.connector
from pymongo import MongoClient
import datetime
import time
from itertools import groupby
import json

#<-------------------------------------- Database connections ------------------------------------------------->

class database_connection:
  def __init__(self,sql_host = "165.22.208.52",sql_user="smartAd",sql_pass="WaDl@#smat1!",default_database="S1001",mongo_url ='mongodb://admin:smartories@165.22.208.52:27017/'):
      self.sql_host = sql_host
      self.sql_user = sql_user
      self.sql_password = sql_pass
      self.database = default_database
      self.mongo_url = mongo_url

  def connect_sql(self):
    try:
      self.sqldb = mysql.connector.connect(host = self.sql_host,
                                          user = self.sql_user,
                                          password = self.sql_password,
                                          database = "S1002")
      return self.sqldb

    except:
      print("Unable to connect sql database") 

  def connect_mongo(self):
    try:
      self.mongodb = MongoClient(self.mongo_url)
      self.mdb = self.mongodb[self.database]
      # self.mdb = self.mongodb["correction_db"]
      return self.mdb
    except:
      print("Unable to connect mongodb")

#<------------------------------------------- Helper functions -------------------------------------------------->

#<------------------------------------------ insert operation --------------------------------------------------->
def  find_part_produced_cycle(part):
  db = database_connection().connect_sql()
  mycursor= db.cursor()
  query = "SELECT part_id,part_produced_cycle FROM settings_part_current WHERE part_id = %s";
  mycursor.execute(query,(part,))
  p = mycursor.fetchall()
  return p
def info_insert_data(machine_id,shift_date,calendar_date,part_id,tool_id,ppc,start_time,end_time,shot_count,no_data):
  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  machine_id = machine_id[0]
  calendar_date = calendar_date
  shift_date = shift_date
  shift_id = getShiftid(shift_date,start_time,calendar_date)
  start_time = start_time
  end_time = end_time
  tool_id = tool_id
  actual_shot_count = shot_count
  
  if no_data >= 1:
    production = ppc
    correction_min_counts = "-"+str(production)
  else:
    production = "Null"
    correction_min_counts = production
  corrections = 0
  correction_notes = " "
  rejection_max_count = production
  rejections = 0
  rejections_notes = " "
  reject_reason = ''
  last_updated_by = ''

  sql_query = "INSERT INTO `pdm_production_info`( `machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`, `part_id`, `tool_id`, `actual_shot_count`, `production`, `correction_min_counts`, `corrections`,`correction_notes`, `rejection_max_counts`, `rejections`, `rejections_notes`, `reject_reason`, `last_updated_by`) VALUES(%s  ,%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s )"
  val = ( machine_id , calendar_date , shift_date , shift_id , start_time , end_time , part_id , tool_id , actual_shot_count , production , correction_min_counts , corrections , correction_notes , rejection_max_count , rejections , rejections_notes , reject_reason , last_updated_by )

  cursor.execute(sql_query,val)
  db_instance.commit()

  print("Data stored in production Info Table.....")
  return

# <------------------------------------------Find the Device Status -------------------------------------------------------->

def find_device_status(machine_gateway):
  device_gateway = machine_gateway.split("/")
  # device_gateway = "/"+device_gateway[1]+"/"+device_gateway[2]+"/"+"1DeviceStatus"
  device_gateway ="/chennai/S1001/SMP01/DeviceStatus"

  db_instance = database_connection().connect_mongo()
  collection = db_instance[device_gateway]
  cur = collection.find({}).sort("_id",-1).limit(1)
  device_status=""
  for i in cur:
    device_status = i
  return device_status

#<------------------------------------- update past records operations ------------------------------------------------------->
def update_past_rec(offline_gateway,past_data):
  past_data.sort(key=lambda x: x["gateway_time"])

  # device_state = find_device_status(offline_gateway)
  # time_update = str(device_state['updated_on']).split(" ")
  # time_update=time_update[0]+" "+time_update[1]
  # time_update = datetime.datetime.strptime(str(time_update), '%Y-%m-%d %H:%M:%S')
  # time_update_date = time_update.date()
  # time_update_time = time_update.time().hour
  # # date_temp = datetime.datetime.strptime(past_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")

  # if device_state['device_status']=="Online":
  #   if device_state['meta_data']['is_device_powered_off'] == True:
  #     print(device_state['meta_data']['is_device_powered_off'])
  # else:
  #   print(device_state['device_status'])

  while(True):
    if len(past_data)>1:
      f_rec= past_data[0]
      machine = offline_gateway.split("offline")[0]+str(f_rec['machine_id'])
      machine_data = getTabledetails(machine,past_data[0]['gateway_time'])

      date_time= datetime.datetime.strptime(f_rec['gateway_time'], "%Y-%m-%d %H:%M:%S")

      total_rec=1;
      for rec in past_data[1:]:
        date_time_rec=datetime.datetime.strptime(rec['gateway_time'], "%Y-%m-%d %H:%M:%S")
        if ((f_rec['machine_id'] == rec['machine_id']) and (date_time.date() == date_time_rec.date()) and (date_time.strftime("%H") == date_time_rec.strftime("%H"))):
          total_rec = total_rec+1
          past_data.remove(rec)
      past_data.remove(past_data[0])
      s_time= date_time.strftime("%H")+":"+"00"+":"+"00"
      # # Find the record in info Table
      db_instance = database_connection().connect_sql()
      cursor = db_instance.cursor()
      sql_query2 = "SELECT * FROM `pdm_production_info` WHERE `machine_id`= %s and `shift_date`=%s and `start_time` <=%s ORDER BY r_no DESC LIMIT 1"
      cursor.execute(sql_query2,(machine_data[0][0],date_time.date(),s_time,))
      previous_data = cursor.fetchone()
      # # Find the part produced cycle
      if previous_data is not None:
        sql_query = "SELECT * FROM `settings_part_current` WHERE `part_id`= %s"
        cursor.execute(sql_query,(previous_data[9],))
        part_data = cursor.fetchone()
        # Shot Count
        shot_count = int(total_rec) + int(previous_data[11])
        # Production value
        production= shot_count*part_data[3]

        # Update the production values
        sql_query1 = "UPDATE `pdm_production_info` SET `actual_shot_count`=%s,`production`=%s, `correction_min_counts`=%s,`rejection_max_counts`=%s,`corrections`=%s,`rejections`=%s,`correction_notes`=%s,`rejections_notes`=%s,`reject_reason`=%s WHERE `r_no`=%s"
        cursor.execute(sql_query1,(shot_count,production,production,production,"0","0","","","",previous_data[0],))
        db_instance.commit()
      print("Record updated"," ",date_time_rec.date()," ",s_time)
    else:
      break

def split_past_future(active_records):
  present_data = []
  future_data = []
  past_data = []

  current_hour = int(datetime.datetime.now().strftime("%H"))-1
  current_end_hour = int(datetime.datetime.now().strftime("%H"))
  current_date = datetime.datetime.now().strftime("%Y-%m-%d")
  
  for i in active_records :
    data_arr = i['gateway_time'].split()
    gate_way_date = data_arr[0]
    gate_way_time = data_arr[1].split(':')
    gateway_date_array_split = gate_way_date.split('-')
    current_date_array_split = current_date.split('-')
    if(gate_way_date == current_date):
      if(int(gate_way_time[0]) == int(current_hour)):
        present_data.append(i)
      elif(int(gate_way_time[0]) > int(current_hour)):
        future_data.append(i)
      else:
        past_data.append(i)

    elif((int(gateway_date_array_split[0])>int(current_date_array_split[0])) and (int(gateway_date_array_split[1])>= int(current_date_array_split[1])) and (int(gateway_date_array_split[2])>=int(current_date_array_split[2]))):
      future_data.append(i)
    else:
      past_data.append(i) 
  return present_data,past_data,future_data


def getTabledetails(machine,gateway_time):
  time = gateway_time.split(" ")
  db = database_connection().connect_sql()
  mycursor= db.cursor()
  query = "SELECT i.machine_id,t.machine_id,t.shift_date,t.event_start_time,t.part_id,t.tool_id FROM settings_machine_iot as i INNER JOIN pdm_tool_changeover_log as t ON t.machine_id = i.machine_id WHERE i.iot_gateway_topic = %s and t.shift_date<=%s and t.event_start_time<=%s GROUP BY t.shift_date,t.event_start_time ORDER BY t.last_updated_on DESC LIMIT 1";
  mycursor.execute(query,(machine,time[0],time[1],))
  
  shift = mycursor.fetchall()
  if len(shift)==0:
    db = database_connection().connect_sql()
    mycursor= db.cursor()
    query ="SELECT machine_id FROM `settings_machine_iot` WHERE `iot_gateway_topic`= %s;"
    mycursor.execute(query,(machine,))
    shift = mycursor.fetchall()
  return shift



#<------------------------------------- process data pdm_info------------------------------------------------>

def process_data_pdm_info(offline_gateway,active_records, pdm_start_time, pdm_end_time,no_data):
  present_data,past_data,future_data = split_past_future(active_records)
  if len(present_data)>0:

    print("Process the Present Data")
    file=offline_gateway.replace("/","_")+".txt"
    f= open(file,"a+")
    for x in present_data:
      f.write("Event Timestamp: %s\rmachine_status: %s\rdowntime_status: %s\rstatus: %s\rshot_status: %s\rshot_count: %s\rmachine_id: %s\rgateway_time: %s\r\n" %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),x['machine_status'],x['downtime_status'],x['status'],x['shot_status'],x['shot_count'],x['machine_id'],x['gateway_time']))
    f.close()

  elif len(future_data)>0:

    print("Process the future Data")
    file=offline_gateway.replace("/","_")+".txt"
    f= open(file,"a+")
    for x in future_data:
      f.write("Event Timestamp: %s\rmachine_status: %s\rdowntime_status: %s\rstatus: %s\rshot_status: %s\rshot_count: %s\rmachine_id: %s\rgateway_time: %s\r\n" %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),x['machine_status'],x['downtime_status'],x['status'],x['shot_status'],x['shot_count'],x['machine_id'],x['gateway_time']))
    f.close()

  elif len(past_data)>0:
    # Process for handle the past(offline) data
    update_past_rec(offline_gateway,past_data)

def getNoOfHoursPerShift(start,end):
  start_delta = datetime.timedelta(hours=start.hour, minutes=start.minute, seconds=start.second)
  end_delta = datetime.timedelta(hours=end.hour, minutes=end.minute, seconds=end.second)
  return end_delta - start_delta

def update_list(startTime,noOfHours,id_v,shift_dict,end):
  startTime = (datetime.datetime.min + startTime).time()
  if int(startTime.minute)>0:
    shift_dict[str(str(startTime.hour).zfill(2)+":"+str(startTime.minute).zfill(2)+":00")]=id_v
  else:
    shift_dict[str(str(startTime.hour).zfill(2)+":00:00")]=id_v
  for i in range(1,int(noOfHours.hour)+1):
    if int(startTime.hour+i)>23:
      t = (int(startTime.hour+i)%24)
      if shift_dict[str(str(t).zfill(2)+":00:00")]=="":
        shift_dict[str(str(t).zfill(2)+":00:00")]=id_v 
    else:
      if shift_dict[str(str(startTime.hour+i).zfill(2)+":00:00")]=="":
        shift_dict[str(str(startTime.hour+i).zfill(2)+":00:00")]=id_v
  return shift_dict

# Function for find the shift Shift respective of the timings
def getShiftList(shiftTimings):

  id_v = 'A'
  shift_dict = {}
  for i in range(0,24):
    shift_dict[str(str(i).zfill(2)+":00:00")]=""
  for i in range (0,len(shiftTimings)):
    try:
      try:
        no = str(getNoOfHoursPerShift(shiftTimings[i],shiftTimings[i+1])).split(", ")[1].split(":")
        noOfHours = datetime.timedelta(hours=int(no[0]),minutes=int(no[1]),seconds=int(no[2]))
      except:
        noOfHours = getNoOfHoursPerShift(shiftTimings[i],shiftTimings[i+1])
    except:
      noOfHours = getNoOfHoursPerShift(shiftTimings[i],shiftTimings[0])
    ct = datetime.timedelta(hours=shiftTimings[i].hour, minutes=shiftTimings[i].minute, seconds=shiftTimings[i].second)
    try:
      noOfHours = (datetime.datetime.min + noOfHours).time()
    except:
      noOfHours = str(noOfHours).split(", ")[1]
      noOfHours=datetime.datetime.strptime(noOfHours, "%H:%M:%S").time()
    if(noOfHours.minute==0):
      shift_dict = update_list(ct,noOfHours,id_v,shift_dict,0)
    else:
      shift_dict = update_list(ct,noOfHours,id_v,shift_dict,2)

    id_v = chr(ord(id_v) + 1)
  return shift_dict

#<------------------------------------- process data downtime ------------------------------------------------>

def process_data_pdm_downtime(offline_gateway,collection,pdm_start_time,pdm_end_time):
  device_state = find_device_status(offline_gateway)
  time_update = str(device_state['updated_on']).split(" ")
  time_update=time_update[0]+" "+time_update[1]
  time_update = datetime.datetime.strptime(str(time_update), '%Y-%m-%d %H:%M:%S')
  time_update_date = time_update.date()
  time_update_time = time_update.time().hour
  # date_temp = datetime.datetime.strptime(past_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")

  # if device_state['device_status']=="Online":
  #   if device_state['meta_data']['is_device_powered_off'] == "true":
  #     device_power_start = device_state['meta_data']['device_off_start_time']
  #     device_power_end = device_state['meta_data']['live_data_up']

  #     print("Start="+device_power_start+" End="+device_power_end)
  #   else:
  #     print("Here")
  #     print(device_state['meta_data']['is_device_powered_off'])
  # else:
  #   print(device_state['device_status'])

  present_data,past_data,future_data = split_past_future(collection)
  if len(present_data)>0:
    print("Process the Present Data")
    file=offline_gateway.replace("/","_")+".txt"
    f= open(file,"a+")
    for x in past_data:
      f.write("Event Timestamp: %s\rmachine_status: %s\rdowntime_status: %s\rstatus: %s\rshot_status: %s\rshot_count: %s\rmachine_id: %s\rgateway_time: %s\r\n" %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),x['machine_status'],x['downtime_status'],x['status'],x['shot_status'],x['shot_count'],x['machine_id'],x['gateway_time']))
    f.close()
  elif len(future_data)>0:
    print("Process the future Data")
    file=offline_gateway.replace("/","_")+".txt"
    f= open(file,"a+")
    for x in past_data:
      f.write("Event Timestamp: %s\rmachine_status: %s\rdowntime_status: %s\rstatus: %s\rshot_status: %s\rshot_count: %s\rmachine_id: %s\rgateway_time: %s\r\n" %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),x['machine_status'],x['downtime_status'],x['status'],x['shot_status'],x['shot_count'],x['machine_id'],x['gateway_time']))
    f.close()
  elif len(past_data)>0:
    while(True):
      if len(past_data)>0:
        first_rec = past_data[0]
        date_time= datetime.datetime.strptime(first_rec['gateway_time'], "%Y-%m-%d %H:%M:%S")
        present_data=[]
        for rec in past_data[1:]:
          date_time_rec=datetime.datetime.strptime(rec['gateway_time'], "%Y-%m-%d %H:%M:%S")
          if ((first_rec['machine_id']==rec['machine_id']) and (date_time.date() == date_time_rec.date()) and (date_time.strftime("%H") == date_time_rec.strftime("%H"))):
            present_data.append(rec)
            past_data.remove(rec)
        past_data.remove(past_data[0])
      else:
        break
      l= len(present_data)
      machine = offline_gateway.split("offline")[0]+str(first_rec['machine_id'])
      s=0 
      c = 0
      if l>0: #Condition to check whether the present data present in the present data bucket
        j=0
        flag_s=0
        timestamp = present_data[0]['gateway_time'].split(" ")
        start_time = timestamp[1]
        while(j<l): #This loop will help to cumulate the Next next occurence of the Active Data record
          t_tamp = present_data[j]['gateway_time'].split(" ")
          previous_date = timestamp[0]
          end_time=start_time
          end_date = previous_date
          k=j+1
          event = present_data[j]['status']
          if event == "Active":    
            while (k<l):
              timestamp = present_data[k]['gateway_time'].split(" ")
              end_time = timestamp[1]
              end_date=timestamp[0]
              if present_data[k]['status'] == "Active":
                k=k+1
              else:
                break
          else:
            if(k<l):
              timestamp = present_data[k]['gateway_time'].split(" ")
              end_time = timestamp[1]
              end_date=timestamp[0]
            else:
              timestamp = present_data[k-1]['gateway_time'].split(" ")
              end_time = timestamp[1]
              end_date=timestamp[0]
          #condition to overcome the array index out of bound exception.....
          if k<l:
            shot_count = present_data[k]['shot_count']
          else:
            shot_count = present_data[k-1]['shot_count']
          j=k
          temp_start = start_time.split(":")
          temp_end = end_time.split(":")

          t_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
          t_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))
          # temp_duration = int(int(int(t_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(t_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
          # if temp_duration<0:
          x_date=previous_date.split("-")
          y_date=end_date.split("-")
          a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
          b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
          c_t = b_t-a_t
          temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
          temp_min = int(temp_duration/60)
          temp_sec = int(temp_duration%60)
          duration = str(temp_min)+"."+str(temp_sec)
          reason_mapped = 0; # This is the default value for the parameter
          is_split = 0; # This is the default value for the parameter
          shift_id = getShiftid(shiftTimings,shift_list,start_time)
          # Logics for find the last record 
          if c == 0: #This condition for update the each first record of the each shift hour
            # Find the shift starting hours
            for d in shiftTimings:
              d = str(d).split(":")
              x = str(pdm_end_time).split(":")
              d = int(int(24 if int(d[0])==0 else d[0]))*3600+int(d[1])*60+int(d[2])
              x = int(int(24 if int(x[0])==0 else x[0])-1)*3600+int(x[1])*60+int(x[2])
              if int(d) == int(x):
                shift_list_list = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
                list_index = shift_list_list.index(shift_id)
                shift_start_duration = shiftTimings[list_index]
                if(list_index == 0):
                  shift_end_duration = shiftTimings[len(shiftTimings)-1]
                else:
                  shift_end_duration = shiftTimings[list_index-1]

                db_instance = database_connection().connect_sql()
                cursor = db_instance.cursor()
                sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s ORDER BY r_no DESC LIMIT 1"
                cursor.execute(sql_query2,(machine_id,shift_date,))
                previous_data = cursor.fetchone()
                if previous_data is not None:
                  previous_start = previous_data[9]
                  previous_end = previous_data[10]
                  previous_duration = previous_data[13]
                  previous_rno = previous_data[0]
                  previous_event_id = previous_data[1]

                  temp_start = previous_start.split(":")
                  if previous_data[12]==event:
                    temp_end = str(end_time).split(":")
                    # if event !=present_data[k]['status']:
                    flag_s=1
                  else:
                    temp_end = str(start_time).split(":")

                  temp_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
                  temp_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

                  # temp_duration = int(int(int(temp_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(temp_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
                  # if temp_duration<0:
                  x_date=previous_data[2].split("-")
                  y_date=end_date.split("-")
                  a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
                  b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
                  c_t = b_t-a_t
                  temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
                  
                  temp_min = int(temp_duration/60)
                  temp_sec = int(temp_duration%60)
                  pre_duration = str(temp_min)+"."+str(temp_sec)

                  if previous_data[12]==event:
                    # Update the previous shift end Record......
                    sql_query1 = "UPDATE `pdm_events` SET `shot_count`=%s,`end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                    cursor.execute(sql_query1,(shot_count,end_time,pre_duration,previous_rno,))
                    db_instance.commit()

                    # Update it in Reason mapping Table
                    if previous_data[12] != "Active":
                      sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                      cursor.execute(sql_query2,(end_time,pre_duration,previous_event_id,))
                      db_instance.commit()
                  else:
                    # Update the previous shift end Record......
                    sql_query1 = "UPDATE `pdm_events` SET `shot_count`=%s,`end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                    cursor.execute(sql_query1,(shot_count,start_time,pre_duration,previous_rno,))
                    db_instance.commit()

                    # Update it in Reason mapping Table
                    if previous_data[12] != "Active":
                      sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                      cursor.execute(sql_query2,(start_time,pre_duration,previous_event_id,))
                      db_instance.commit()
                  c = 1
          db_instance = database_connection().connect_sql()
          cursor = db_instance.cursor()
          temp_var=0
          temp_var_same=0
          if(s==0 and c==0): # This loop will execute only once to update the previous inserted data record at the begining of the each hour execution
            cursor.execute("SELECT * FROM `pdm_events` WHERE `machine_id` like %s ORDER BY r_no DESC LIMIT 1;",(('%'+machine_id,)))
            previous_data = cursor.fetchone()
            if previous_data is not None: #Condition if the first data not inserted (If already data exist condition true otherwise false)
              previous_start = previous_data[9]
              previous_end = previous_data[10]
              previous_duration = previous_data[13]
              previous_rno = previous_data[0]
              previous_event_id = previous_data[1]
              temp_start = previous_start.split(":")
              temp_end = start_time.split(":")
              temp_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
              temp_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

              # temp_duration = int(int(int(temp_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(temp_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
              # if temp_duration<0:
              x_date=previous_data[2].split("-")
              y_date=end_date.split("-")
              a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
              b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
              c_t = b_t-a_t
              temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
              
              temp_min = int(temp_duration/60)
              temp_sec = int(temp_duration%60)
              pre_duration = str(temp_min)+"."+str(temp_sec)

              if previous_data[12]==event: # Conditions for check the previous data event same as current event.......
                temp_end = end_time.split(":")
                temp_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
                temp_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

                # temp_duration = int(int(int(temp_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(temp_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
                # if temp_duration<0:
                x_date=previous_data[2].split("-")
                y_date=end_date.split("-")
                a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
                b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
                c_t = b_t-a_t
                temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

                temp_min = int(temp_duration/60)
                temp_sec = int(temp_duration%60)
                pre_duration = str(temp_min)+"."+str(temp_sec)
                # print(start_time," ",end_time," ",previous_data[9])
                sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(end_time,pre_duration,previous_rno,))
                db_instance.commit()
                if previous_data[12] != "Active":
                  sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query2,(end_time,pre_duration,previous_event_id,))
                  db_instance.commit()
                temp_var=1
              else: # If previous and Current data event not same, insert the current data
                sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(start_time,pre_duration,previous_rno,))
                db_instance.commit()
                if previous_data[12] != "Active":
                  sql_query1 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query1,(start_time,pre_duration,previous_event_id,))
                  db_instance.commit()

              temp_var_same=1
            s=j
          # This is for insert the current data records
          # update the no data issue
          if (j==l and flag_s!=1): # Conditions to check if the current record is the last record of the hour......
            tmp_var2=0
            for idx, s_time in enumerate(shiftTimings):
              if(datetime.datetime.strptime(str(s_time), "%H:%M:%S").time().hour == (datetime.datetime.strptime(str(pdm_end_time), "%H:%M:%S").time().hour)):
                temp_start = str(start_time).split(":")
                temp_end = str(s_time).split(":")
                t_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
                t_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

                # temp_duration = int(int(int(t_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(t_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
                # if temp_duration<0:
                x_date=previous_date.split("-")
                y_date=end_date.split("-")
                a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
                b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
                c_t = b_t-a_t
                temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

                temp_min = int(temp_duration/60)
                temp_sec = int(temp_duration%60)
                duration = str(temp_min)+"."+str(temp_sec)
                end_time =s_time
                if temp_var_same !=0:
                  cursor.execute("SELECT * FROM `pdm_events` WHERE `machine_id` like %s ORDER BY r_no DESC LIMIT 1;",(('%'+machine_id,)))
                  previous_data = cursor.fetchone()
                  
                  previous_rno = previous_data[0]
                  previous_event_id = previous_data[1]
                  if previous_data[12]==event:
                    duration = find_duration(previous_data[2],end_date,previous_data[9],s_time)
                    sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                    cursor.execute(sql_query1,(s_time,duration,previous_rno,))
                    db_instance.commit()
                    if previous_data[12] != "Active":
                      sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                      cursor.execute(sql_query2,(s_time,duration,previous_event_id,))
                      db_instance.commit()
                  else:
                    duration = find_duration(previous_data[2],end_date,previous_data[9],start_time)
                    sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                    cursor.execute(sql_query1,(start_time,duration,previous_rno,))
                    db_instance.commit()
                    if previous_data[12] != "Active":
                      sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                      cursor.execute(sql_query2,(start_time,duration,previous_event_id,))
                      db_instance.commit()

                    duration = find_duration(end_date,end_date,start_time,s_time)
                    # Newly added condition
                    sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s)"
                    val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id )
                    cursor.execute(sql_query,val)
                  
                else:
                  # Insert the current shift record of last record
                  sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s)"
                  val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id )
                  cursor.execute(sql_query,val)
              
                start_time=s_time
                end_time=s_time
                duration=0
                shift_id = getShiftid(shiftTimings,shift_list,start_time)
                
                end_time_t = str(calendar_date)+" "+str(end_time)
                # shift_date = getShiftdate(datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S'))
                shift_date = getShiftdate(datetime.datetime.strptime(((datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S"))        

                sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s)"
                val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id )
                cursor.execute(sql_query,val)
                db_instance.commit()
                tmp_var2=1
                break
            if ((temp_var !=1) and (tmp_var2 !=1)):
              sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s)"
              val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id )
              cursor.execute(sql_query,val)
              db_instance.commit()
          elif(temp_var !=1 and flag_s!=1):
            sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s)"
            val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id )
            cursor.execute(sql_query,val)
            db_instance.commit()
          
          flag_s=0
          start_time = str(end_time)
          print("Data stored in downtime event tables......")
  return

#<------------------------------------- process data ------------------------------------------------>

def process_data(offline_gateway,collection, duration_start = 0, duration_end = 0):

  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(duration_end != 0):
    e_hrs = s_hrs

  pdm_start_time = str(s_hrs).zfill(2)+":"+str(duration_start).zfill(2)+":"+"00"
  pdm_end_time = str(e_hrs).zfill(2)+":"+str(duration_end).zfill(2)+":"+"00"  

  active_records = []

  collection.sort(key=lambda x: x["status"])
  groups = groupby(collection, lambda x: x['status'])
  for status, group in groups:
    if(status == "Active"):
      for content in group:
        active_records.append(content)
  # process_data_pdm_info(offline_gateway,active_records,pdm_start_time,pdm_end_time,len(collection))

  collection.sort(key=lambda x: x["gateway_time"])

  process_data_pdm_downtime(offline_gateway,collection,pdm_start_time,pdm_end_time)
  print("Process completed..!")
  return 1

#<------------------------------------- on 14/07/2022 ------------------------------------------------>

#<------------------------------------- Get shift info ------------------------------------------------>

def getShiftinfo(db_instance,gateway_time):
  cursor = db_instance.cursor()
  cursor.execute("SELECT `shift_log_id`  FROM `settings_shift_management` WHERE `last_updated_on`<= %s ORDER BY `last_updated_on` DESC LIMIT 1;",(gateway_time,))
  shift_log_id = cursor.fetchall()
  if len(shift_log_id)>0:
    shift_log_id = shift_log_id[0][0].split("f")
    shift_suffix = shift_log_id[1]
    return shift_suffix
  else:
    shift_suffix="01"
    return shift_suffix  
#<------------------------------------- on 11/07/2022 ---------------------------------------------------->


#<------------------------------------- Get shift Timings ------------------------------------------------>
  
def getShiftTimings(db_instance,gateway_time):
  cursor = db_instance.cursor()
  sql_query = "SELECT * FROM `settings_shift_table` WHERE `Shifts` like %s"
  cursor.execute(sql_query,(('%'+getShiftinfo(database_connection().connect_sql(),gateway_time),)))
  shifts = cursor.fetchall()
  cursor.close()
  arr = []
  for count, value in enumerate(shifts):
    arr.append((datetime.datetime.min + value[1]).time())
  return arr


#<------------------------------------- on 12/07/2022 --------------------------------------------------->

#<------------------------------------- Get shift id ------------------------------------------------>
def getShiftid(shiftTimings,shift_list,time):
  shift="A"
  for s in shiftTimings:
    time=datetime.datetime.strptime(str(time), "%H:%M:%S").time()
    if int(time.hour) == int(s.hour):
      if (int(s.minute)>0) and (int(time.minute)>=int(s.minute)):
        shift = shift_list[str(time.hour).zfill(2)+":"+str(s.minute).zfill(2)+":00"]
        break
      elif (int(s.minute)>0) and (int(time.minute)>=int(s.minute)):
        shift = shift_list[str(str(time.hour).zfill(2)+":00:00")]
        break
    else:
      shift = shift_list[str(str(time.hour).zfill(2)+":00:00")]
  return shift
#<------------------------------------- Get Machine info ------------------------------------------------>
def getMachineinfo(db_instance):
  cursor = db_instance.cursor()
  query = "SELECT `iot_gateway_topic` FROM `settings_machine_iot`"
  cursor.execute(query)
  machines = cursor.fetchall()
  arr = []
  for count, value in enumerate(machines):
    arr.append(value)
  return arr  


#<------------------------------------- on 12/07/2022 --------------------------------------------------->


#<------------------------------------- Get shift date -------------------------------------------------->

def getShiftdate(gateway_time):
  shiftTimings = getShiftTimings(database_connection().connect_sql(),gateway_time)
  now = datetime.datetime.strptime(gateway_time, "%Y-%m-%d %H:%M:%S")
  shift_date = datetime.datetime.strptime(gateway_time, "%Y-%m-%d %H:%M:%S")
  #if(int(now.strftime("%H")) <= int(shiftTimings[0].strftime("%H")) and int(now.strftime("%M")) <= int(shiftTimings[0].strftime("%M"))):
  if(now.time()<=shiftTimings[0]):
    shift_date =  now - datetime.timedelta(days=1)
    shift_date = shift_date.strftime("%Y-%m-%d")
  else:
    shift_date = shift_date.strftime("%Y-%m-%d")
  return shift_date


#<------------------------------------- on 13/07/2022 -------------------------------------------------->

# <----------------------------------------------created by naveen status ADDING ROW DATA-------------------------------------------------------->
def add_status_raw_data(lst):
    working_list = []
    
    for i in lst:
        # print(i)
        if i['downtime_status'] == True and i['machine_status'] == True:
            status = "Inactive"
        elif i['downtime_status'] == False and i['machine_status'] == True:
            status = "Active"
        else:
            status = "Machine OFF"

        data_list = {
            "downtime_status" : i['downtime_status'],
            "machine_status" : i['machine_status'],
            "machine_id" : i['machine_id'],
            "shot_count" : i['shot_count'],
            "shot_status" : i['shot_staus'],
            "gateway_time" : i['gateway_time'],
            "status" : status,
        }
        # print(data_list)
        working_list.append(data_list)
    return working_list

# <---------------------------------------------------END OF STATUS ADDING FUNCTION---------------------------------------------------->

#<------------------------------------- Get rawData ----------------------------------------------------->

def getRawData(machine,split = 0,split_start = 0, split_end = 0):

  now = datetime.datetime.now()
  db_instance = database_connection().connect_mongo()
  collection = db_instance[machine]

  # This line of code will take current hour and substract 1 from it 
  # If the current hour is "00" this meaning 24 so, we have take current hour-1 as a starting and current hour as ending hour
  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(split!= 0 and int(split_end) != 0):
    e_hrs = s_hrs

  # This is for trial case....
  # start_time = "2022-08-03 08:00:00"
  # end_time = "2022-08-03 10:00:00"

  start_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  end_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(e_hrs).zfill(2)+":"+str(split_end).zfill(2)+":"+"00"
  
  start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
  end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
  
  cur = collection.find({"updated_on":{'$gte':start_time ,'$lt':end_time}})
  lst = [i['data'] for i in cur]
  return lst

#<------------------------------------- on 13/07/2022 ------------------------------------------------>


#<------------------------------------- Main Function ------------------------------------------------>

if __name__ == '__main__':
  #<---------------------- Loop break daywise --------sss----------------->
  while(True):
    now = datetime.datetime.now() # Take current time to check the current shift hours
    if(int(now.strftime("%M"))>=0): # you can change time here to run the code
  #<------------------- Site wise data processing ------------------->
      print("trigger")
      offline = "/chennai/S1001/offline"
      collection = getRawData(offline)
      process_data(offline,collection, duration_start = 0, duration_end = 0)
#<------------------------------------- end of processing hourly ------------------------------------------------>
      time.sleep(65)                  
#<------------------------------------------------------ end ---------------------------------------------------->




