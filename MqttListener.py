import pymongo
import paho.mqtt.client as mqttClient
import json
import time
from datetime import datetime

class Mqtt:

	def __init__(self,site):
		self.json_data = {}
		self.site = site
		self.conn = pymongo.MongoClient('mongodb://admin:smartories@165.22.208.52:27017/')
		self.db = self.conn[self.site]
		mqttclient = mqttClient.Client("5667")
		mqttclient.on_connect = self.on_connect
		mqttclient.on_message = self.on_message
		mqttclient.username_pw_set(username="quantanics",password="quantanics")
		mqttstatus = mqttclient.connect("209.97.177.78", 1883,60)
		mqttclient.loop_forever()

	def on_connect(self,mqttclient, userdata, flags,rc):
		if rc == 0:
			mqttclient.subscribe("#",2)
		else:
			print("Connection failed")
        
	def on_message(self, mqttclient, userdata, msg):
		mqtt_msg = str(msg.payload).replace("b'", "").replace("'", "").replace("  ", "").replace("\n", "")
		mqtt_msg = str(mqtt_msg).replace("\\n","")
		json_data = json.loads(mqtt_msg)
		collection = self.db[msg.topic]
		ts = time.time()
		isodate = datetime.fromtimestamp(ts, None)
		data = {"updated_on":isodate,"data":json_data}
		collection.insert_one(data)
		print(data)

Mqtt('S1001')

