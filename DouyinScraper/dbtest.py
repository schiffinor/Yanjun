import database

db = database.Database("test", "test.json")

db.new_user({"zama": "test", "name": "test"})
db.new_user({"zama": "test1", "name": "test1"})