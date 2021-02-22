import logging
logging.basicConfig(filename="ttnv3_thingsboard.log", format='%(asctime)s %(levelname)s - %(message)s', level=logging.DEBUG)
import config
import paho.mqtt.client as mqtt
import time
import json

ttnv3_thingsboard_plugin_version = "0.1.0"
ttnv3_thingsboard_plugin_configFile = "ttnv3_thingsboard.yml"

def on_connect(client, userdata, flags, rc):
    if (rc == 0):
        print("\t\tClient {:s} connected with success!!!".format(client._client_id.decode("utf-8")))
        logging.info("Client %s connected with success!!!", client._client_id.decode("utf-8"))
        if "TTN" in client._client_id.decode("utf-8"):
            sub = "v3/"+client._username.decode("utf-8")+"/devices/+/up"            
            client.subscribe(sub)            
    elif (rc == 1):
        print("\t\tClient {:s} connection refused - incorrect protocol version!!!".format(client._client_id.decode("utf-8")))
        logging.error("Client %s connection refused - incorrect protocol version!!!", client._client_id.decode("utf-8"))
    elif (rc == 2):
        print("\t\tClient {:s} connection refused - invalid client identifier!!!".format(client._client_id.decode("utf-8")))
        logging.error("Client %s connection refused - invalid client identifier!!!", client._client_id.decode("utf-8"))
    elif (rc == 3):
        print("\t\tClient {:s} connection refused - server unavailable!!!".format(client._client_id.decode("utf-8")))
        logging.error("Client %s connection refused - server unavailable!!!", client._client_id.decode("utf-8"))
    elif (rc == 4):
        print("\t\tClient {:s} connection refused - bad username or password!!!".format(client._client_id.decode("utf-8")))
        logging.error("Client %s connection refused - bad username or password!!!", client._client_id.decode("utf-8"))
    elif (rc == 5):
        print("\t\tClient {:s} connection refused - not authorized!!!".format(client._client_id.decode("utf-8")))
        logging.error("Client %s connection refused - not authorized!!!", client._client_id.decode("utf-8"))
    else:
        print("\t\tClient {:s} connection refused - unknown error!!!".format(client._client_id.decode("utf-8")))
        logging.error("Client %s connection refused - unknown error!!!", client._client_id.decode("utf-8"))

def on_subscribe(client, userdata, mid, granted_qos):
    print("\t\tClient {:s} uplink subscribed!!!".format(client._client_id.decode("utf-8")))
    logging.info("Client %s uplink subscribed", client._client_id.decode("utf-8"))

def on_publish(client, userdata, mid):
    print("\tClient {:s} publish a message".format(client._client_id.decode("utf-8")))

def on_message(client, userdata, msg):
    print("\t\t\tClient {:s} receive a message from topic {:s}".format(client._client_id.decode("utf-8"), msg.topic))
    logging.info("Client %s receive a message from topic %s", client._client_id.decode("utf-8"), msg.topic)
    # print("\tMessage: {:s}".format(msg.payload.decode("utf-8")))
    # jsonData = json.loads(msg.payload.decode("utf-8"))
    jsonData = msg.payload.decode("utf-8")
    jsonObj = json.loads(jsonData)    
    device_id = jsonObj["end_device_ids"]["device_id"]
    thingsboard = mqtt.Client(userdata+"-Thingsboard")    
    thingsboard.username_pw_set(device_id+cfgObj.getAccessTokenComplement(userdata), "")        
    thingsboard.on_connect = on_connect
    thingsboard.on_publish = on_publish
    thingsboard.connect(cfgObj.getThingsboardHost())
    # thingsboard.publish("v1/devices/me/telemetry", payload=jsonData)
    thingsboard.loop_start()
    thingsboard.disconnect()

def main():
    print("Starting communication between TTNv3 and Thingsboard server plugin version", ttnv3_thingsboard_plugin_version)
    logging.info("Starting communication between TTNv3 and Thingsboard server plugin version %s", ttnv3_thingsboard_plugin_version)
    print("\tReading configuration file... ", end='')
    global cfgObj
    cfgObj = config.Config(ttnv3_thingsboard_plugin_configFile)
    _proj = cfgObj.getProjects()
    print("[OK]")

    # Create MQTT client handlers
    mqttClients = []    
    for id in _proj:
        print("\tCreating MQTT client to project "+str(id))
        ttn = mqtt.Client(id+"-TTN")        
        ttn.username_pw_set(cfgObj.getAPIKeyName(id), cfgObj.getAPIKeyID(id))
        ttn.user_data_set(id)
        ttn.on_connect = on_connect
        ttn.on_subscribe = on_subscribe
        ttn.on_message = on_message
        ttn.connect(cfgObj.getTTNHost(), cfgObj.getTTNPort(), 60)        
        ttn.loop_start()

    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()