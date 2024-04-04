from src.db.mongo_connector import MongoConnector

mongo_connector = MongoConnector()
mongo = mongo_connector.get_database("financials")
