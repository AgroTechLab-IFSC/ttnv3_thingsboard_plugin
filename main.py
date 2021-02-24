import logging
logging.basicConfig(filename="ttnv3_thingsboard.log", format='%(asctime)s %(levelname)s - %(message)s', level=logging.DEBUG)
import config
import project

ttnv3_thingsboard_plugin_version = "0.1.0"
ttnv3_thingsboard_plugin_configFile = "ttnv3_thingsboard.yml"

def main():
    print("Starting communication between TTNv3 and Thingsboard server plugin version", ttnv3_thingsboard_plugin_version)
    logging.info("Starting communication between TTNv3 and Thingsboard server plugin version %s", ttnv3_thingsboard_plugin_version)
    print("\tReading configuration file... ", end='')
    global cfgObj
    cfgObj = config.Config(ttnv3_thingsboard_plugin_configFile)
    _proj = cfgObj.getProjects()
    print("[OK]")

    print("\tCreating and launching projects threads...")
    projVect = []   
    i = 0 
    for id in _proj:
        projVect.append(project.Project(id, cfgObj))
        projVect[i].start()
        i += 1       

if __name__ == "__main__":
    main()