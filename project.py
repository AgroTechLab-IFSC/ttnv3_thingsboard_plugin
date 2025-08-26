import threading
import logging
import paho.mqtt.client as mqtt
import ssl
import json


class Project(threading.Thread):

    def __init__(self, name, cfg):
        threading.Thread.__init__(self)
        self.name = name
        self.config = cfg
    
    def parse_ttn_payload(self, jsonObject, fport):
        payload = "{"

        # Check if "decoded_payload" exists
        if "decoded_payload" not in jsonObject["uplink_message"]:
            logging.warning("Decoded payload not into received message")
        else:
            decoded_payload = jsonObject["uplink_message"]["decoded_payload"]
            for id in decoded_payload:
                payload += "\""+id+"\":\""+str(jsonObject["uplink_message"]["decoded_payload"][id])+"\""
                if (id != list(decoded_payload.keys())[-1]):
                    payload += ", "
 
        # Some data are only updated for FPort 1 (attributes)
        if (fport == 1):
                
            # Check if "dev_eui" exists
            if "dev_eui" not in jsonObject["end_device_ids"]:
                logging.warning("Device EUI not into received message")
            else:
                payload += ", \"dev_eui\":\""+str(jsonObject["end_device_ids"]["dev_eui"])+"\""

            # Check if "join_eui" exists
            if "join_eui" not in jsonObject["end_device_ids"]:
                logging.warning("Join EUI not into received message")
            else:
                payload += ", \"join_eui\":\""+str(jsonObject["end_device_ids"]["join_eui"])+"\""

            # Check if "dev_addr" exists
            if "dev_addr" not in jsonObject["end_device_ids"]:
                logging.warning("Device address not into received message")
            else:
                payload += ", \"dev_addr\":\""+str(jsonObject["end_device_ids"]["dev_addr"])+"\""

        # Some data are only updated for FPort 2 (telemetry)
        elif (fport == 2):
            # Check if "rx_metadata" exists
            if "rx_metadata" not in jsonObject["uplink_message"]:
                logging.warning("RX metadata not into received message")
            else:
                # Get SNR
                if "snr" not in jsonObject["uplink_message"]["rx_metadata"][0]:
                    logging.warning("SNR not into received message")
                else:
                    payload += ", \"snr\":"+str(jsonObject["uplink_message"]["rx_metadata"][0]["snr"])
            
                # Get RSSI
                if "rssi" not in jsonObject["uplink_message"]["rx_metadata"][0]:
                    logging.warning("RSSI not into received message")
                else:
                    payload += ", \"rssi\":"+str(jsonObject["uplink_message"]["rx_metadata"][0]["rssi"])
                
                # Get Channel RSSI
                if "channel_rssi" not in jsonObject["uplink_message"]["rx_metadata"][0]:
                    logging.warning("Channel RSSI not into received message")
                else:
                    payload += ", \"channel_rssi\":"+str(jsonObject["uplink_message"]["rx_metadata"][0]["channel_rssi"])
            
            # Check if "setting" exists
            if "settings" not in jsonObject["uplink_message"]:
                logging.warning("Settings not into received message")
            else:
                # Get frequency
                if "frequency" not in jsonObject["uplink_message"]["settings"]:
                    logging.warning("Frequency not into received message")
                else:
                    payload += ", \"frequency\":"+str(jsonObject["uplink_message"]["settings"]["frequency"])

                # Get bandwidth
                if "bandwidth" not in jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]:
                    logging.warning("Bandwidth not into received message")
                else:
                    payload += ", \"bandwidth\":"+str(jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]["bandwidth"])

                # Get spreading factor
                if "spreading_factor" not in jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]:
                    logging.warning("Spreading factor not into received message")
                else:
                    payload += ", \"spreading_factor\":"+str(jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]["spreading_factor"])

                # Get coding rate
                if "coding_rate" not in jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]:
                    logging.warning("Coding rate not into received message")
                else:
                    payload += ", \"coding_rate\":\""+str(jsonObject["uplink_message"]["settings"]["data_rate"]["lora"]["coding_rate"])+"\""

            # Get consumed_airtime
            if "consumed_airtime" not in jsonObject["uplink_message"]:
                logging.warning("Consumed airtime not into received message")
            else:
                payload += ", \"consumed_airtime\":"+str(jsonObject["uplink_message"]["consumed_airtime"]).removesuffix('s')

        payload += "}"
        return payload

    def pub_tb(self, jsonObj):
        # Get LoRaWAN FPort from message
        try:
            fport = jsonObj["uplink_message"]["f_port"]
        except Exception as e:
            logging.error("LoRaWAN FPort no defined at TTN message received: %s", e)
            thingsboard.disconnect()
            return

        # Publish message to Thingsboard based on FPort value
        # FPort 1 is for device attributes
        # FPort 2 is for device telemetry
        if (fport == 1):
            thingsboard.publish("v1/devices/me/attributes", payload=self.parse_ttn_payload(jsonObj, fport))
        elif (fport == 2):
            thingsboard.publish("v1/devices/me/telemetry", payload=self.parse_ttn_payload(jsonObj, fport))
        else:
            logging.warning("FPort %d not managed", fport)
        

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if (reason_code == mqtt.CONNACK_ACCEPTED):
            print("\t\t\tClient {:s} connected with success!!!".format(client._client_id.decode("utf-8")))
            logging.debug("Client %s connected with success!!!", client._client_id.decode("utf-8"))
            if "TTN" in client._client_id.decode("utf-8"):
                sub = "v3/"+client._username.decode("utf-8")+"/devices/+/up"            
                client.subscribe(sub)
            if "Thingsboard" in client._client_id.decode("utf-8"):
                self.pub_tb(jsonObj)
        elif (reason_code == mqtt.CONNACK_REFUSED_PROTOCOL_VERSION):
            print("\t\t\tClient {:s} connection refused - incorrect protocol version!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - incorrect protocol version!!!", client._client_id.decode("utf-8"))
        elif (reason_code == mqtt.CONNACK_REFUSED_IDENTIFIER_REJECTED):
            print("\t\t\tClient {:s} connection refused - invalid client identifier!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - invalid client identifier!!!", client._client_id.decode("utf-8"))
        elif (reason_code == mqtt.CONNACK_REFUSED_SERVER_UNAVAILABLE):
            print("\t\t\tClient {:s} connection refused - server unavailable!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - server unavailable!!!", client._client_id.decode("utf-8"))
        elif (reason_code == mqtt.CONNACK_REFUSED_BAD_USERNAME_PASSWORD):
            print("\t\t\tClient {:s} connection refused - bad username or password!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - bad username or password!!!", client._client_id.decode("utf-8"))
        elif (reason_code == mqtt.CONNACK_REFUSED_NOT_AUTHORIZED):
            print("\t\t\tClient {:s} connection refused - not authorized!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - not authorized!!!", client._client_id.decode("utf-8"))
        else:
            print("\t\t\tClient {:s} connection refused - unknown error!!!".format(client._client_id.decode("utf-8")))
            logging.error("Client %s connection refused - unknown error!!!", client._client_id.decode("utf-8"))

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        if (reason_code == mqtt.MQTT_ERR_SUCCESS):
            print("\t\t\tClient {:s} successful disconnect from MQTT broker".format(client._client_id.decode("utf-8")))
            logging.debug("Client %s successful disconnect from MQTT broker", client._client_id.decode("utf-8"))
        else:
            print("\t\t\tClient {:s} unexpected disconnect from MQTT broker".format(client._client_id.decode("utf-8")))
            logging.warning("Client %s unexpected disconnect from MQTT broker", client._client_id.decode("utf-8"))
            if "TTN" in client._client_id.decode("utf-8"):
                ttn.connect(self.config.getTTNHost(), self.config.getTTNPort())                

    def on_subscribe(self, client, userdata, mid, reason_codes, properties):
        print("\t\t\tClient {:s} uplink subscribed!!!".format(client._client_id.decode("utf-8")))
        logging.debug("Client %s uplink subscribed", client._client_id.decode("utf-8"))

    def on_unsubscribe(self, client, userdata, mid, reason_codes, properties):
        print("\t\t\tClient {:s} uplink unsubscribed!!!".format(client._client_id.decode("utf-8")))
        logging.debug("Client %s uplink unsubscribed", client._client_id.decode("utf-8"))

    def on_publish(self, client, userdata, mid, reason_codes, properties):
        print("\t\t\tClient {:s} publish a message".format(client._client_id.decode("utf-8")))
        logging.debug("Client %s publish a message", client._client_id.decode("utf-8"))
        if "Thingsboard" in client._client_id.decode("utf-8"):
                thingsboard.disconnect()

    def on_message(self, client, userdata, msg):
        print("\t\t\tClient [{:s}] received a message from topic [{:s}]".format(client._client_id.decode("utf-8"), msg.topic))
        logging.debug("Client [%s] received a message from topic [%s]", client._client_id.decode("utf-8"), msg.topic)                
        jsonData = msg.payload.decode("utf-8")        
        global jsonObj
        jsonObj = json.loads(jsonData)
        # print(jsonObj)

        # Get LoRaWAN device ID
        try:
            device_id = jsonObj["end_device_ids"]["device_id"]
        except Exception as e:
            logging.error("Device ID not defined at TTN message received: %s", e)
            return

        # Connect to Thingsboard to publish message received from TTN
        try:
            thingsboard.username_pw_set(username=device_id+self.config.getAccessTokenComplement(userdata))
            thingsboard.connect(self.config.getThingsboardHost(), self.config.getThingsboardPort())
        except Exception as e:
            logging.error("Error connecting to Thingsboard: %s", e)
        
    def run(self):
        # Start thread
        logging.debug("Starting %s project thread", self.name)
        print("\t\tStarting {} project thread".format(self.name))
        
        # Creates TTN object and its parameters 
        logging.debug("Creating TTN client to project {}".format(self.name))
        print("\t\t\tCreating TTN client to project "+self.name)
        global ttn
        ttn = mqtt.Client(client_id=self.name+"-TTN", callback_api_version=mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
        ttn.username_pw_set(self.config.getAPIKeyName(self.name), self.config.getAPIKeyID(self.name))
        ttn.user_data_set(self.name)
        ttn.on_connect = self.on_connect
        ttn.on_disconnect = self.on_disconnect
        ttn.on_subscribe = self.on_subscribe
        ttn.on_unsubscribe = self.on_unsubscribe
        ttn.on_publish = self.on_publish
        ttn.on_message = self.on_message
        ttn.tls_set(ca_certs=None, tls_version=ssl.PROTOCOL_TLS)

        # Creates Thingsboard client and its parameters
        logging.debug("Creating Thingsboard client to project {}".format(self.name))
        print("\t\t\tCreating Thingsboard client to project "+self.name)
        global thingsboard
        thingsboard = mqtt.Client(client_id=self.name+"-Thingsboard", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)                
        thingsboard.on_connect = self.on_connect
        thingsboard.on_disconnect = self.on_disconnect
        thingsboard.on_publish = self.on_publish        

        # Connect to TTN broker and run process looping
        ttn.connect(host=self.config.getTTNHost(), port=self.config.getTTNPort(), keepalive=120)       
        while True:
            ttn.loop_read()
            ttn.loop_write()
            ttn.loop_misc()            
            thingsboard.loop_read()
            thingsboard.loop_write()
            thingsboard.loop_misc()