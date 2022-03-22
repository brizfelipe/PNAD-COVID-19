import os 


class AcessPostgres:
    
    engine = os.environ.get("ENGINE")
    dbName = os.environ.get("DATABASE")
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    host = os.environ.get("HOST")
    port = os.environ.get("PORT")
    