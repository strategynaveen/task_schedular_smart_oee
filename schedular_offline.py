########################################## Task Schedular Event ###############################################

#<-------------------------------------------- Imports -------------------------------------------------------->
import mysql.connector
from pymongo import MongoClient
import datetime
import time
from itertools import groupby
import json

# For identify the current timestamp
import datetime
import pytz
from pytz import timezone

import logging
import traceback

filename ="schedular_demo_offline_log/"+"all_prod_offline_log"+str(time.time())+".log"
logging.basicConfig(filename=filename,
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#<-------------------------------------- Database connections ------------------------------------------------->

class database_connection:
  def __init__(self,sql_host = "localhost",sql_user="root",sql_pass="",default_database="s1001",mongo_url ='mongodb://localhost:27017/'):
  # def __init__(self,sql_host = "localhost",sql_user="root",sql_pass="",default_database="S1001",mongo_url ='mongodb://smtechadmin:admin%40smtech@smartories.com:27017/'):
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
                                          database = "s1001")
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
    if production == 0:
      correction_min_counts = 0
    else:
      correction_min_counts = "-"+str(production)
  else:
    production = None
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

def find_device_status(machine_gateway,split = 0,split_start = 0, split_end = 0):
  
  device_gateway = machine_gateway.split("/")
  # device_gateway = "/"+device_gateway[1]+"/"+device_gateway[2]+"/"+"1DeviceStatus"
  device_gateway ="/chennai/S1001/device_status/SMD001"

  db_instance = database_connection().connect_mongo()
  collection = db_instance[device_gateway]

  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(split!= 0 and int(split_end) != 0):
    e_hrs = s_hrs

  end_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(e_hrs).zfill(2)+":"+str(split_end).zfill(2)+":"+"00"
  temp_start = (datetime.datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
  start_time = str(datetime.datetime.strptime(str(temp_start), '%Y-%m-%d %H:%M:%S').date())+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  
  start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
  end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

  cur = collection.find({"updated_on":{'$gte':start_time ,'$lt':end_time}})
  lst = [i for i in cur]
  return lst

#<------------------------------------- update past records operations ------------------------------------------------------->
def update_past_rec(offline_gateway,past_data):
  past_data.sort(key=lambda x: x["gateway_time"])
  while(True):
    if len(past_data)>=1:
      f_rec= past_data[0]
      machine = offline_gateway.split("offline")[0]+str(f_rec['machine_id'])
      machine_data = getTabledetails(machine,past_data[0]['gateway_time'])

      date_time= datetime.datetime.strptime(f_rec['gateway_time'], "%Y-%m-%d %H:%M:%S")

      total_rec=1;
      date_time_rec=datetime.datetime.strptime(past_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S")
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
      sql_query2 = "SELECT * FROM `pdm_production_info` WHERE `machine_id`= %s and `shift_date`=%s and `start_time` =%s and `hierarchy`='parent' LIMIT 1"
      cursor.execute(sql_query2,(machine_data[0][0],date_time.date(),s_time,))
      previous_data = cursor.fetchone()
      # # Find the part produced cycle

      if previous_data is not None:
        sql_query = "SELECT * FROM `settings_part_current` WHERE `part_id`= %s"
        cursor.execute(sql_query,(previous_data[8],))
        part_data = cursor.fetchone()
        # Shot Count
        shot_count = int(total_rec) + int(previous_data[10])
        # Production value
        production= int(shot_count)*int(part_data[3])

        # Update the production values
        sql_query1 = "UPDATE `pdm_production_info` SET `actual_shot_count`=%s,`production`=%s, `correction_min_counts`=%s,`rejection_max_counts`=%s,`corrections`=%s,`rejections`=%s,`correction_notes`=%s,`rejections_notes`=%s,`reject_reason`=%s WHERE `r_no`=%s"
        cursor.execute(sql_query1,(shot_count,production,("-"+str(production)),production,"0","0","","","",previous_data[0],))
        db_instance.commit()

        # Update all the child records respective of the parent record.
        sql_query1 = "UPDATE `pdm_production_info` SET `actual_shot_count`=%s,`production`=%s, `correction_min_counts`=%s,`rejection_max_counts`=%s,`corrections`=%s,`rejections`=%s,`correction_notes`=%s,`rejections_notes`=%s,`reject_reason`=%s WHERE `hierarchy`=%s"
        cursor.execute(sql_query1,(shot_count,production,("-"+str(production)),production,"0","0","","","",previous_data[1],))
        db_instance.commit()

        logger.info("*********")
        logger.info("Production Data updated for")
        logger.info(machine_data[0][0])
        logger.info("Production Count")
        logger.info(total_rec)
        logger.info("*********")

      print("Record updated"," ",date_time_rec.date()," ",s_time)
    else:
      break

def find_duration(start_date,end_date,start_time,end_time):
  temp_start = str(start_time).split(":")
  temp_end = str(end_time).split(":")

  x_date=str(start_date).split("-")
  y_date=str(end_date).split("-")
  a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
  b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
  c_t = b_t-a_t
  temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

  temp_min = int(temp_duration/60)
  temp_sec = int(temp_duration%60)
  duration = str(temp_min)+"."+str(temp_sec)
  return duration


def split_past_future(hour,active_records):
  end_time =datetime.datetime.strptime(((datetime.datetime.strptime(str(hour), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S")

  present_data = []
  future_data = []
  past_data = []

  current_hour = datetime.datetime.strptime(str(hour), "%Y-%m-%d %H:%M:%S").time().hour
  current_end_hour = datetime.datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S").time().hour
  current_date=str(datetime.datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S").date())
  if int(current_end_hour)==0:
    current_date = (datetime.datetime.strptime(current_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

  for i in active_records :
    data_arr = i['gateway_time'].split()
    gate_way_date = data_arr[0]
    gate_way_time = data_arr[1].split(':')
    gateway_date_array_split = gate_way_date.split('-')
    current_date_array_split = current_date.split('-')

    if(gate_way_date == current_date):
      if(int(gate_way_time[0]) == int(current_hour)):
        present_data.append(i)
      elif(int(gate_way_time[0])==23):
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
  query = "SELECT t.machine_id,t.tool_changeover_id,t.no_of_part,t.tool_id,t.shift_date,t.calendar_date,t.event_start_time,t.shift_id,t.machine_event_id,t.last_updated_by,t.last_updated_on FROM pdm_tool_changeover as t INNER JOIN settings_machine_iot as s on t.machine_id=s.machine_id WHERE s.iot_gateway_topic = %s and t.shift_date<=%s and t.event_start_time<=%s ORDER BY t.shift_date DESC,t.event_start_time DESC, t.last_updated_on DESC LIMIT 1;"
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
  if len(active_records)>0:
    update_past_rec(offline_gateway,active_records)

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

def process_data_pdm_downtime(machine,offline_gateway,past_data,pdm_start_time,pdm_end_time,split,split_start,split_end):
  device_state = find_device_status(offline_gateway,split,split_start,split_end)
  serial_id = str(machine).split("/")
  for d in device_state:
    if(d['data']['device_status'] == "Online"):
      if(d['data']['meta_data']['is_device_powered_off'] == 'True'):
        time_update = str(d['data']['meta_data']['device_off_start_time']).split(" ")
        dev_start = d['data']['meta_data']['device_off_start_time']
        dev_end = d['data']['meta_data']['device_off_end_time']
        time_update = datetime.datetime.strptime(str(dev_start), '%Y-%m-%d %H:%M:%S')
        devicedict = {
          'status': 'No Data',
          'shot_status': 0,
          'shot_count': 0,
          'machine_id': serial_id[3], 
          'gateway_time': str(time_update) 
        }
        past_data.append(devicedict)
        time_update = datetime.datetime.strptime(str(dev_end), '%Y-%m-%d %H:%M:%S')
        devicedict = { 
          'status': 'Offline',
          'shot_status': 0,
          'shot_count': 0,
          'machine_id': serial_id[3], 
          'gateway_time': str(time_update) 
        }
        past_data.append(devicedict)
  past_data.sort(key=lambda x: datetime.datetime.strptime(str(x["gateway_time"]), "%Y-%m-%d %H:%M:%S"), reverse=False)

  past_data_temp=[]
  for rec in past_data:
    if (str(rec['machine_id']) == str(serial_id[3])):
      past_data_temp.append(rec)
  past_data = past_data_temp

  if len(past_data)>0:
    while(True):
      if len(past_data)>0:
        first_rec = past_data[0]
        date_time= datetime.datetime.strptime(first_rec['gateway_time'], "%Y-%m-%d %H:%M:%S")
        present_data=[]
        present_data.append(first_rec)
        for rec in past_data[1:]:
          date_time_rec=datetime.datetime.strptime(rec['gateway_time'], "%Y-%m-%d %H:%M:%S")
          if ((first_rec['machine_id']==rec['machine_id']) and (date_time.date() == date_time_rec.date()) and (date_time.strftime("%H") == date_time_rec.strftime("%H"))):
            present_data.append(rec)
            past_data.remove(rec)
        past_data.remove(past_data[0])
      else:
        break
      l= len(present_data)
      date_temp = datetime.datetime.strptime(present_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
      machine = offline_gateway.split("offline")[0]+str(first_rec['machine_id'])
      s=0
      c = 0
      t=0
      if l>0: #Condition to check whether the present data present in the present data bucket
        j=0
        flag_s=0
        timestamp = present_data[0]['gateway_time'].split(" ")
        end_time = timestamp[1]
        timestamp_gateway = present_data[0]['gateway_time']
        while(j<l): #This loop will help to cumulate the Next next occurence of the Active Data record
          t_tamp = present_data[j]['gateway_time'].split(" ")
          previous_date = timestamp[0]
          end_date = previous_date
    
          event = present_data[j]['status']
          event_g = event
          timestamp_gateway = present_data[j]['gateway_time']

          if(j<l):
            timestamp_gateway = present_data[j]['gateway_time']
            timestamp = present_data[j]['gateway_time'].split(" ")
            end_time = timestamp[1]
            end_date=timestamp[0]
          else:
            timestamp_gateway = present_data[j-1]['gateway_time']
            timestamp = present_data[j-1]['gateway_time'].split(" ")
            end_time = timestamp[1]
            end_date=timestamp[0]
          #condition to overcome the array index out of bound exception.....
          if j<l:
            shot_count = present_data[j]['shot_count']
          else:
            shot_count = present_data[j-1]['shot_count']
          if j==l:
            end_time = str(pdm_end_time)
          j=j+1

          event_g = event
          shiftTimings = getShiftTimings(database_connection().connect_sql(),present_data[0]['gateway_time'])
          shift_list = getShiftList(shiftTimings)
          shift_id = getShiftid(shiftTimings,shift_list,end_time)

          shift = getTabledetails(machine,present_data[0]['gateway_time'])
          machine_id = shift[0][0]
          shift_date = getShiftdate(present_data[0]['gateway_time'])
          calendar_date = datetime.datetime.strptime(present_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
          source = "Offline"
          timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(end_time)), '%Y-%m-%d %H:%M:%S')

          db_instance = database_connection().connect_sql()
          cursor = db_instance.cursor()
          sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s and `timestamp`<=%s ORDER BY `timestamp` DESC LIMIT 1"
          cursor.execute(sql_query2,(machine_id,shift_date,timestamp_t,))
          previous_data = cursor.fetchone()

          if previous_data is not None:
            start_time = previous_data[10]
            # duration = find_duration(previous_date,end_date,start_time,end_time)
      
            previous_data_t=previous_data
            previous_start = previous_data[9]
            previous_end = previous_data[10]
            previous_end_t =  previous_data[10]
            previous_duration = previous_data[13]
            previous_rno = previous_data[0]
            previous_event_id = previous_data[1]

            # If previous event is same as current event, do not do anything in offline schedular.....
            if previous_data[12] != event_g:
              if previous_data[12] != "Offline":
                sql_query1 = "UPDATE `pdm_events` SET `event`=%s , `source`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(event_g,"Offline",previous_rno,))
                db_instance.commit()
              else:
                shift_date = previous_data[3]
                duration = find_duration(shift_date,shift_date,previous_start,end_time)
                # Update the previous record
                sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s , `source`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(end_time,duration,"Offline",previous_rno,))
                db_instance.commit()

                if previous_data[12] != "Active":
                  sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query2,(end_time,duration,previous_event_id,))
                  db_instance.commit()

                # # Update the current Record.
                start_time = end_time
                end_time = previous_end
                duration = find_duration(shift_date,shift_date,start_time,end_time)

                part_id=previous_data[7]
                tool_id=previous_data[6]
                event = event_g
                timestamp_t1 = datetime.datetime.strptime(str(str(previous_data_t[2])+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
                sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` ,`timestamp`) VALUES(%s, %s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s,%s)"
                val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event_g,duration , "0" , "0" ,part_id, tool_id , source , timestamp_t1)
                cursor.execute(sql_query,val)
                db_instance.commit()
            else:
              logger.info("*********")
              logger.info("Event already Exist!")
              logger.info(machine_id)
              logger.info(event_g)
              logger.info(start_time)
              logger.info("*********")
          else:
            print("previous data not found!")
        print("Data stored in downtime event tables......")
      else:
        break
  return

#<------------------------------------- process data ------------------------------------------------>

def process_data(machines,offline_gateway,collection, duration_start = 0, duration_end = 0,split = 0,split_start = 0, split_end = 0):
  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  pdm_start_time = str(s_hrs).zfill(2)+":"+str(duration_start).zfill(2)+":"+"00"
  pdm_end_time = str(e_hrs).zfill(2)+":"+str(duration_end).zfill(2)+":"+"00"

  active_records = []

  collection.sort(key=lambda x: x["status"])
  groups = groupby(collection, lambda x: x['status'])
  for status, group in groups:
    if(status == "Active"):
      for content in group:
        active_records.append(content)
  process_data_pdm_info(offline_gateway,active_records,pdm_start_time,pdm_end_time,len(collection))
  
  collection.sort(key=lambda x: x["gateway_time"])
  for count,value in enumerate(machines):
    machine = value[0]
    print(machine)
    process_data_pdm_downtime(machine,offline_gateway,collection,pdm_start_time,pdm_end_time,split,split_start,split_end)
  print(pdm_start_time)
  print(pdm_end_time)
  print("Process completed..!")
  return 1

#<------------------------------------- on 22/06/2023 ------------------------------------------------>
def get_offline_data(db_instance,gateway,gateway_time):
  cursor = db_instance.cursor()
  # query = "SELECT t.* FROM pdm_events as t INNER JOIN settings_machine_iot as s on t.machine_id=s.machine_id WHERE s.iot_gateway_topic = %s and t.shift_date<=%s and t.event_start_time<%s ORDER BY t.shift_date DESC,t.event_start_time DESC, t.last_updated_on DESC LIMIT 1;"
  query = "SELECT t.* FROM pdm_events as t INNER JOIN settings_machine_iot as s on t.machine_id=s.machine_id WHERE s.iot_gateway_topic = %s and t.event ='Offline' ORDER BY shift_date ASC;"
  cursor.execute(query,(gateway,))
  all_rec = cursor.fetchall()
  return all_rec

def process_data_offline(machines,offline_gateway,collection_data, duration_start = 0, duration_end = 0,split = 0,split_start = 0, split_end = 0):

  for collection in collection_data:
    if collection['meta_data']['is_empty'] == True:
      connected_device = collection['meta_data']['connected_device']
      connected_device = str(connected_device).replace("{",'')
      connected_device = str(connected_device).replace("}",'')
      connected_device = str(connected_device).split(",")

      for k in connected_device:
        g_way = str(machines[0][0]).split("/")
        g_way[3]= str(k);
        g_way = "/".join(g_way)
        # g_way_time = d['data']['meta_data']['gateway_time']
        g_way_time = str(collection['meta_data']['gateway_time'])
    
        # Get the Offline Records......
        offline_data = get_offline_data(database_connection().connect_sql(),g_way,g_way_time)
        db_instance = database_connection().connect_sql()
        cursor = db_instance.cursor()
        for x in offline_data:
          # query = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s ORDER BY r_no DESC LIMIT 1"
          query = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s and `machine_event_id` < %s and `r_no`<%s ORDER BY r_no DESC LIMIT 1"
          cursor.execute(query,(x[4],x[3],x[1],x[0],))
          all_rec = cursor.fetchall()

          if all_rec[0][5] == x[5] and all_rec[0][3] == x[3]:

            temp_start = str(all_rec[0][9]).split(":")
            temp_end = str(x[10]).split(":")

            x_date=str(x[3]).split("-")
            y_date=str(x[3]).split("-")
            a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
            b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
            c_t = b_t-a_t
            temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

            temp_min = int(temp_duration/60)
            temp_sec = int(temp_duration%60)
            pre_duration = str(temp_min)+"."+str(temp_sec).zfill(2)

            sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
            cursor.execute(sql_query1,(x[10],pre_duration,all_rec[0][0],))
            db_instance.commit()

            if all_rec[0][12] != "Active":
              sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
              cursor.execute(sql_query2,(x[10],pre_duration,all_rec[0][1],))
              db_instance.commit()

            # Delete the current record
            sql_query3 = "UPDATE `pdm_events` SET `calendar_date`='0000-00-00',`shift_date`='0000-00-00',`machine_id`='',`shift_id`='',`tool_id`='',`part_id`='',`record_created_on`='0000-00-00 00:00:00',`start_time`='',`end_time`='',`shot_count`=0,`event`='',`duration`=0,`reason_mapped`=0,`is_split`=0,`timestamp`='0000-00-00 00:00:00',`source`='Offline' WHERE `r_no`=%s"
            cursor.execute(sql_query3,(x[0],))
            db_instance.commit()

            sql_query4 = "UPDATE `pdm_downtime_reason_mapping` SET `machine_id`='',`split_id`='',`calendar_date`='0000-00-00',`shift_date`='0000-00-00',`Shift_id`='',`start_time`='',`end_time`='',`downtime_reason_id`='',`split_duration`=0,`tool_id`='',`part_id`='',`notes`='',`last_updated_by`='',`last_updated_on`='0000-00-00 00:00:00' WHERE `machine_event_id`=%s"
            cursor.execute(sql_query4,(x[1],))
            db_instance.commit()

          else:
            sql_query1 = "UPDATE `pdm_events` SET `event`=%s WHERE `r_no`=%s"
            cursor.execute(sql_query1,(all_rec[0][12],x[0],))
            db_instance.commit()

            # Delete if Active
            if all_rec[0][12] == "Active":
              sql_query5 = "UPDATE `pdm_downtime_reason_mapping` SET `machine_id`='',`split_id`='',`calendar_date`='0000-00-00',`shift_date`='0000-00-00',`Shift_id`='',`start_time`='',`end_time`='',`downtime_reason_id`='',`split_duration`=0,`tool_id`='',`part_id`='',`notes`='',`last_updated_by`='',`last_updated_on`='0000-00-00 00:00:00' WHERE `machine_event_id`=%s"
              cursor.execute(sql_query5,(x[1],))
              db_instance.commit()
  print("Process completed..!")
  return 1

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
  hour_now = datetime.datetime.strptime(gateway_time, "%Y-%m-%d %H:%M:%S")
  shift_date = hour_now.strftime("%Y-%m-%d")
  now = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
  s_time = datetime.datetime.strptime(str(hour_now.date())+" "+str(shiftTimings[0]), "%Y-%m-%d %H:%M:%S")
  # if(int(hour_now.strftime("%H")) <= int(shiftTimings[0].strftime("%H")) and int(hour_now.strftime("%M")) <= int(shiftTimings[0].strftime("%M"))):
  if(hour_now <= s_time):
    shift_date =  datetime.datetime.strptime(((datetime.datetime.strptime(str(hour_now), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S")
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

def getRawData(gateway,split = 0,split_start = 0, split_end = 0):

  now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
  db_instance = database_connection().connect_mongo()
  collection = db_instance[gateway]
  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(split!= 0 and int(split_end) != 0):
    e_hrs = s_hrs

  # start_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  end_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(e_hrs).zfill(2)+":"+str(split_end).zfill(2)+":"+"00"
  temp_start = (datetime.datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
  start_time = str(datetime.datetime.strptime(str(temp_start), '%Y-%m-%d %H:%M:%S').date())+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  
  start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
  end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
  cur = collection.find({"updated_on":{'$gte':start_time ,'$lt':end_time}})
  lst = [i['data'] for i in cur]
  return lst

def getRawDataStatus(gateway,split = 0,split_start = 0, split_end = 0):

  now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
  db_instance = database_connection().connect_mongo()
  collection = db_instance[gateway]
  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(split!= 0 and int(split_end) != 0):
    e_hrs = s_hrs

  # start_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  end_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(e_hrs).zfill(2)+":"+str(split_end).zfill(2)+":"+"00"
  temp_start = (datetime.datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
  start_time = str(datetime.datetime.strptime(str(temp_start), '%Y-%m-%d %H:%M:%S').date())+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  
  start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
  end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
  cur = collection.find({"updated_on":{'$gte':start_time ,'$lt':end_time}})
  lst = [i['data'] for i in cur]
  return lst

#<------------------------------------- on 13/07/2022 ------------------------------------------------>

def getMachineinfo(db_instance):
  cursor = db_instance.cursor()
  query = "SELECT `iot_gateway_topic` FROM `settings_machine_iot`"
  cursor.execute(query)
  machines = cursor.fetchall()
  arr = []
  for count, value in enumerate(machines):
    arr.append(value)
  return arr

#<------------------------------------- Main Function ------------------------------------------------>

if __name__ == '__main__':
  machines = getMachineinfo(database_connection().connect_sql())
  #<---------------------- Loop break daywise --------sss----------------->
  while(True):
    now = datetime.datetime.now(pytz.timezone('Asia/Kolkata')) # Take current time to check the current shift hours
    try:
      if(int(now.strftime("%M"))==0): # you can change time here to run the code
        time.sleep(5)
        logger.info("connected")
        logger.info(now)
    #<------------------- Site wise data processing ------------------->
        # print("trigger")
        offline_gateway = "/chennai/S1001/offline"

        print("trigger")
        collection = getRawData(offline_gateway)
        process_data(machines,offline_gateway,collection, duration_start = 0, duration_end = 0,split = 0,split_start = 0, split_end = 0)
        
        # Function for Update the Offline Event Data....
        offline_status_gateway = "/chennai/S1001/offline_data_bucket_status/SMD001"
        print("Offline Data Status processing....")
        collect_offline = getRawDataStatus(offline_status_gateway)
        process_data_offline(machines,offline_status_gateway,collect_offline, duration_start = 0, duration_end = 0,split = 0,split_start = 0, split_end = 0)
  #<------------------------------------- end of processing hourly ------------------------------------------------>
        time.sleep(65)
    except BaseException as err:
      logger.info(err)
      logger.info(traceback.format_exc())
      time.sleep(60)
#<------------------------------------------------------ end ---------------------------------------------------->
