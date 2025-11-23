

from pathlib import Path
from mysql.connector import Error

def create_mongodb_schema(db):
    db.drop_collection("users")
    db.create_collection("users", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "login"],
            "properties": {
                "user_id": {
                    "bsonType": "int"
                    },
                "login" : {
                    "bsonType" : "string"
                    },
                "gravatar_id" : {
                    "bsonType": ["string", "null"]
                    },
                "avatar_url" : {
                    "bsonType": ["string", "null"]
                    },
                "url" : {
                    "bsonType": ["string", "null"]
                    }
                }
            }
        })
    db.Users.create_index("user_id",unique = True)

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    # print(collections)
    if "users" not in collections:
        raise ValueError("-----------Missing collection in MongoDB------------")
    user = db.users.find_one({"user_id":1})
    if not user:
        raise ValueError("-----------user_id not found in MongoDB------------")
    print("--------validated schema in MongoDB----------")

SQL_FILE_PATH = Path("../sql/schema.sql")
TRIGGER_FILE_PATH = Path("/home/prime/PycharmProjects/DE-ETL-102/sql/trigger.sql")

def create_mysql_schema(connection, cursor):

    database = "vanhaydoi"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    connection.commit()
    print(f"---------Created database {database} in Mysql--------")
    connection.database = database
    try:
        with open(SQL_FILE_PATH, "r") as file:
            sql_script = file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"----------Executed Mysql commands -----------")
            connection.commit()
            print("--------Created Mysql Schema-----------")
    except Error as e:
        connection.rollback()
        raise Exception(f"----------Failed to create Mysql Schema: {e}-------") from e

def create_mysql_trigger(connection, cursor):
    database = "github_data"
    connection.database = database
    try:
        with open(TRIGGER_FILE_PATH, "r") as file:
            sql_script = file.read()
            delimiter = 'DELIMITER //'
            statements = sql_script.split(delimiter)
            for statement in statements:
                if statement.strip():
                    # If the statement contains the trigger definition, handle the custom delimiter
                    if 'CREATE TRIGGER' in statement.upper():
                        cursor.execute('DELIMITER //')
                        trigger_sql = statement.split('DELIMITER ;')[0].strip()
                        cursor.execute(trigger_sql)
                        cursor.execute('DELIMITER ;')
                    else:
                        # Execute other statements (e.g., CREATE TABLE)
                        cursor.execute(statement)
                    connection.commit()
                    print("SQL file executed successfully.")
    except Error as e:
        connection.rollback()
        raise Exception(f"----------Failed to create Mysql Schema: {e}-------") from e



def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    # print(cursor.fetchall())
    tables = [row[0] for row in cursor.fetchall()]
    if "users" not in tables or "repositories" not in tables:
        raise ValueError(f"----------Table  doesn't exist--------")
    cursor.execute("SELECT * FROM users WHERE user_id =1")

    # print(cursor.fetchone())
    user = cursor.fetchone()
    if not user:
        raise ValueError("-------user not found--------")
    print("--------validated schema in MySql----------")


 # "user_id":1,
    #         "login":"GoogleCodeExporter",
    #         "gravatar_id":"",
    #         "avatar_url": "https://avatars.githubusercontent.com/u/9614759?",
    #         "url":"https://api.github.com/users/GoogleCodeExporter"

def create_redis_schema(client):
    try:
        client.flushdb() # drop database
        client.set("user:1:login","GoogleCodeExporter")
        client.set("user:1:gravatar_id", "")
        client.set("user:1:avatar_url", "https://avatars.githubusercontent.com/u/9614759?")
        client.set("user:1:url", "https://api.github.com/users/GoogleCodeExporter")
        client.sadd("user_id", "user:1")
        print("--------------add data to Redis suscessfully------------")
    except Exception as e:
        raise Exception(f"-------Failed to add data to Redis: {e}------") from e

def validate_redis_schema(client):

    if not client.get("user:1:login") == "GoogleCodeExporter":
        raise ValueError("-------Value login not found in Redis ---------")

    if not client.sismember("user_id", "user:1"):
        raise ValueError("---------User not set in Redis--------")

    print("--------validated schema in Redis----------")