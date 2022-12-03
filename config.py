import cag.utils.config as graph_config

# For demo purposes, these creadentials are set to default 
class Config(graph_config.Config):
    def __init__(self):
        self.insightsnet_config = graph_config.Config(
            url= "http://127.0.0.1:8529",
            user= "root",
            password= "p3yqy8I0dHkpOrZs",
            database= "_system",
            graph= "CAGWikipediaGraph"
        )