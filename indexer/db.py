import pymongo
import os


class MongoManager:
    __instance = None

    @staticmethod
    def get_db_instance():
        if MongoManager.__instance is None:
            MongoManager()
        return MongoManager.__instance

    def __init__(self):
        if MongoManager.__instance is not None:
            raise Exception("Error. Cannot re-instantiate a singleton")
        else:
            if 'MONGO_PASSWORD' in os.environ:
                MongoManager.__instance = pymongo.MongoClient(
                    f"mongodb://{os.environ['MONGO_USERNAME']}:"
                    f"{os.environ['MONGO_PASSWORD']}@"
                    f"{os.environ['MONGO_HOST']}:27017/{os.environ['MONGO_DB_NAME']}"
                )[os.environ['MONGO_DB_NAME']]

            else:
                MongoManager.__instance = pymongo.MongoClient(
                    'mongodb://localhost:27017',
                ).synindexdb