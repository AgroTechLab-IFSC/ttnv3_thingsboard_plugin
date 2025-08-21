import sys
import logging
from ruamel.yaml import YAML

class Config:

    def __init__(self, _cfgFile):
        self.configFile = _cfgFile
        self.tree = self._readConfigFile()
    
    def _readConfigFile(self):
        try:
            with open(self.configFile, 'r') as _f:
                yaml = YAML(typ='safe')
                tree = yaml.load(_f)
                return tree
        except FileNotFoundError:
            logging.error("Configuration file not found!!!")
            sys.exit("ERROR: configuration file not found!!!")
    
    def getProjects(self):
        logging.debug("Getting 'Projects' key information")
        if "Projects" not in self.tree:
            logging.error("Not 'Projects' key found on configuration file")
            sys.exit("ERROR: Not 'Projects' key found on configuration file!!!")
        _projects = self.tree["Projects"]
        logging.info("%s project(s) found: %s", len(_projects), _projects)
        return _projects
    
    def getAPIKeyName(self, _projectName):
        return self.tree[_projectName]["api_key_name"]
    
    def getAPIKeyID(self, _projectName):
        return self.tree[_projectName]["api_key_id"]
    
    def getTTNHost(self):
        return self.tree["TTNv3"]["host"]        
    
    def getTTNPort(self):
        return self.tree["TTNv3"]["port"]
    
    def getThingsboardHost(self):
        return self.tree["Thingsboard"]["host"]
    
    def getThingsboardPort(self):
        return self.tree["Thingsboard"]["port"]
    
    def getAccessTokenComplement(self, _projectName):
        return self.tree[_projectName]["access_token_complement"]