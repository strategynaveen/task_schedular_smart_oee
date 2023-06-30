import pymongo
import paho.mqtt.client as mqttClient
from threading import Thread
import json
import time
from datetime import datetime
import logging

filename ="machine_log/"+"all_machine_log"+str(time.time())+".log"
logging.basicConfig(filename=filename,
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class Mqtt:
	def __init__(self,site):
		self.json_data = {}
		self.site = site
		self.conn = pymongo.MongoClient('mongodb://localhost:27017/')
		self.db = self.conn[self.site]
		mqttclient = mqttClient.Client("5668297")
		mqttclient.on_connect = self.on_connect
		mqttclient.on_message = self.on_message
		mqttclient.username_pw_set(username="quantanics",password="quantanics123")
		mqttstatus = mqttclient.connect("165.22.208.52", 1883,60)
		mqttclient.subscribe("#",2)
		mqttclient.loop_forever()

	def upload(self,msg):
		print(msg)
		mqtt_msg = str(msg.payload).replace("b'", "").replace("'", "").replace("  ", "").replace("\\n", "").replace("\n", '')
		mqtt_msg = str(mqtt_msg).replace("\\","")
		#mqtt_msg = str(mqtt_msg).replace("n","")
		mqtt_msg = str(mqtt_msg).replace('}"','}')
		mqtt_msg = str(mqtt_msg).replace('"{','{')
		json_data = json.loads(mqtt_msg)
		collection = self.db[msg.topic]
		ts = time.time()
		isodate = datetime.fromtimestamp(ts, None)
		data = {"updated_on":isodate,"data":json_data}
		collection.insert_one(data)
		logger.info(data)
		print("Data Inserted!")


	def on_connect(self,mqttclient, userdata, flags,rc):
		if rc == 0:
			logger.info("connected")
			print("connected!")
		else:
			logger.info("Connection failed")
        
	def on_message(self, mqttclient, userdata, msg):
		print(msg)
		Thread(target=self.upload, args=(msg,)).start()

		

if __name__ == '__main__':
	Mqtt('S1001')

