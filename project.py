import threading
import logging
import time
import paho.mqtt.client as mqtt
import json


class Project(threading.Thread):

    def __init__(self, name, cfg):
        threading.Thread.__init__(self)
        self.name = name
        self.config = cfg
    
    def processPayload(self, jsonObject):
        payload = "{"                
        payload += "\"f_port\":"+str(jsonObject["uplink_message"]["f_port"])
        payload += ", \"f_cnt\":"+str(jsonObject["uplink_message"]["f_cnt"])
        if "decoded_payload" not in ["uplink_message"]:
            print("\t\t\tDecoded payload not into received message!!!")
            logging.warning("Decoded payload not into received message")
        else:
            telemetry = jsonObject["uplink_message"]["decoded_payload"]
            for id in telemetry:
                payload += ", \""+id+"\":"+str(jsonObject["uplink_message"]["decoded_payload"][id])
        # payload += ", \"data_rate_index\":"+str(jsonObject["uplink_message"]["settings"]["data_rate_index"])
        payload += ", \"coding_rate\":\""+jsonObject["uplink_message"]["settings"]["coding_rate"]+"\""
        payload += ", \"frequency\":"+str(jsonObject["uplink_message"]["settings"]["frequency"])
        payload += ", \"lora_bandwidth\":"+str(jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]["bandwidth"])
        payload += ", \"lora_spreading_factor\":"+str(jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]["spreading_factor"])
        payload += "}"
        # print(payload)
        return payload

    def on_connect(self, client, userdata, flags, rc):
        if (rc == mqtt.CONNACK_ACCEPTED):
            print("\t\t\tClient {:s} connected with success!!!".format(client._client_id.decode("utf-8")))
            logging.debug("Client %s connected with success!!!", client._client_id.decode("utf-8"))
            if "TTN" in client._client_id.decode("utf-8"):
                sub = "v3/"+client._username.decode("utf-8")+"/devices/+/up"            
                client.subscribe(sub)
            if "Thingsboard" in client._client_id.decode("utf-8"):
                thingsboard.publish("v1/devices/me/telemetry", payload=self.processPayload(jsonObj))
        elif (rc == mqtt.CONNACK_REFUSED_PROTOCOL_VERSION):
            print("\t\t\tClient {:s} connection refused - incorrect protocol version!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - incorrect protocol version!!!", client._client_id.decode("utf-8"))
        elif (rc == mqtt.CONNACK_REFUSED_IDENTIFIER_REJECTED):
            print("\t\t\tClient {:s} connection refused - invalid client identifier!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - invalid client identifier!!!", client._client_id.decode("utf-8"))
        elif (rc == mqtt.CONNACK_REFUSED_SERVER_UNAVAILABLE):
            print("\t\t\tClient {:s} connection refused - server unavailable!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - server unavailable!!!", client._client_id.decode("utf-8"))
        elif (rc == mqtt.CONNACK_REFUSED_BAD_USERNAME_PASSWORD):
            print("\t\t\tClient {:s} connection refused - bad username or password!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - bad username or password!!!", client._client_id.decode("utf-8"))
        elif (rc == mqtt.CONNACK_REFUSED_NOT_AUTHORIZED):
            print("\t\t\tClient {:s} connection refused - not authorized!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - not authorized!!!", client._client_id.decode("utf-8"))
        else:
            print("\t\t\tClient {:s} connection refused - unknown error!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - unknown error!!!", client._client_id.decode("utf-8"))

    def on_disconnect(self, client, userdata, rc):
        if (rc == mqtt.MQTT_ERR_SUCCESS):
            print("\t\t\tClient {:s} successful disconnect from MQTT broker".format(client._client_id.decode("utf-8")))
            logging.debug("Client %s successful disconnect from MQTT broker", client._client_id.decode("utf-8"))
        else:
            print("\t\t\tClient {:s} unexpected disconnect from MQTT broker".format(client._client_id.decode("utf-8")))
            logging.warning("Client %s unexpected disconnect from MQTT broker", client._client_id.decode("utf-8"))
            if "TTN" in client._client_id.decode("utf-8"):
                ttn.connect(self.config.getTTNHost(), self.config.getTTNPort())                

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("\t\t\tClient {:s} uplink subscribed!!!".format(client._client_id.decode("utf-8")))
        logging.debug("Client %s uplink subscribed", client._client_id.decode("utf-8"))

    def on_publish(self, client, userdata, mid):
        print("\t\t\tClient {:s} publish a message".format(client._client_id.decode("utf-8")))
        logging.debug("Client %s publish a message", client._client_id.decode("utf-8"))
        if "Thingsboard" in client._client_id.decode("utf-8"):
                thingsboard.disconnect()

    def on_message(self, client, userdata, msg):
        print("\t\t\tClient {:s} receive a message from topic {:s}".format(client._client_id.decode("utf-8"), msg.topic))
        logging.debug("Client %s receive a message from topic %s", client._client_id.decode("utf-8"), msg.topic)                
        jsonData = msg.payload.decode("utf-8")        
        global jsonObj
        jsonObj = json.loads(jsonData)
        print(jsonObj)
        device_id = jsonObj["end_device_ids"]["device_id"]        
        thingsboard.username_pw_set(device_id+self.config.getAccessTokenComplement(userdata), "")          
        thingsboard.connect(self.config.getThingsboardHost(), self.config.getThingsboardPort())                                
        
    def run(self):
        # Start thread
        logging.debug("Starting %s project thread", self.name)
        print("\t\tStarting {} project thread".format(self.name))

        logging.debug("Creating TTN client to project {}".format(self.name))
        print("\t\t\tCreating TTN client to project "+self.name)
        global ttn
        ttn = mqtt.Client(self.name+"-TTN")        
        ttn.username_pw_set(self.config.getAPIKeyName(self.name), self.config.getAPIKeyID(self.name))
        ttn.user_data_set(self.name)
        ttn.on_connect = self.on_connect
        ttn.on_disconnect = self.on_disconnect
        ttn.on_subscribe = self.on_subscribe
        ttn.on_message = self.on_message

        
        logging.debug("Creating Thingsboard client to project {}".format(self.name))
        print("\t\t\tCreating Thingsboard client to project "+self.name)
        global thingsboard
        thingsboard = mqtt.Client(self.name+"-Thingsboard")                
        thingsboard.on_connect = self.on_connect
        thingsboard.on_disconnect = self.on_disconnect
        thingsboard.on_publish = self.on_publish        

        ttn.connect(host=self.config.getTTNHost(), port=self.config.getTTNPort(), keepalive=120)        
        while True:            
            # ttn.loop_read()
            # ttn.loop_write()
            # ttn.loop_misc()            
            # thingsboard.loop_read()
            # thingsboard.loop_write()
            # thingsboard.loop_misc()
            ttn.loop()
            thingsboard.loop()
            # time.sleep(1)
    
    