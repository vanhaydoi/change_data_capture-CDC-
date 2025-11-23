
from database.mysql_connect import MySQLConnect
from config.database_config import get_database_config
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from src.ETL.consumer import consumer
from itertools import zip_longest

def get_data_trigger(mysql_client,last_timestamp):
    try:
            connection, cursor = mysql_client.connection, mysql_client.cursor
            database = "github_data"
            connection.database = database

            query = ("SELECT user_id, login, gravatar_id, avatar_url, url, state, "
                     "  DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp1 "
                     "FROM user_log_after"
                     )

            if last_timestamp:
                #query += " WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s') = DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')"
                query += f" WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') > '{last_timestamp}'"
                cursor.execute(query)
            else:
                cursor.execute(query)

            rows= cursor.fetchall()
            connection.commit()

            schema = ["user_id", "login", "gravatar_id", "avatar_url", "url", "state", "log_timestamp"]
            data = [dict(zip(schema, row)) for row in rows]

            # get last_timestamp lastest
            new_timestamp = max((row["log_timestamp"] for row in data), default=last_timestamp) if data else last_timestamp
            return data, new_timestamp
    except Exception as e:
        print(f"----------Error as : {e}--------")
        return [], last_timestamp


def producer():
    config = get_database_config()
    last_timestamp = None

    total_message_producer = 0

    while True:
        with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:
            # # send to kafka
            producer = KafkaProducer(bootstrap_servers='localhost:9092'
                                     , value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                     )
            while True:
                data, new_timestamp = get_data_trigger(mysql_client, last_timestamp)
                last_timestamp = new_timestamp # max log_timestamp in trigger mysql
                # print(last_timestamp)
                data_producer = []
                for record in data:
                    # time.sleep(1)
                    producer.send('datdepzai', record)
                    total_message_producer += 1
                    producer.flush()
                    # print(record)
                    # print(f"-------------------{total_message_producer}-----------------")
                    data_producer.append(record)
                    #print(data_producer)

                    yield  total_message_producer, data_producer
                # yield data_producer

def tao_du_lieu_so():
    for a in range(5):
        yield a

def consumer(tuplee):

    # consumer()
    #
    x = [4]
    for i in tuplee:
        yield i
        # x.append(i)
        print(x)
        print("------------------")


# def validate_kafka( data_producer, data_consumer):
#     difference = [item for item in data_producer if item not in data_consumer]
#     if difference:
#         producer = KafkaProducer(bootstrap_servers='localhost:9092'
#                                  , value_serializer=lambda v: json.dumps(v).encode('utf-8')
#                                  )
#
#         for data in difference:
#             producer.send(data, "datdepzai")
#             print(difference)
#             producer.flush()
#
#     print("-------validate pass ---------")




def main():
    # count_messages_producer, data_producer = next(producer())
    # count_messages_consumer, data_consumer = next(consumer())
    #
    # if count_messages_producer == count_messages_consumer:
    #     validate_kafka(data_producer,data_consumer )
    # else:
    #     print("--------missing data---------")
    #     validate_kafka(data_producer,data_consumer)

    # for (count_messages_producer, data_producer), (count_messages_consumer, data_consumer) in zip(producer(), consumer()):
    #     yield "--------------------------"
    #
    #     if count_messages_producer == count_messages_consumer:
    #         difference = [item for item in data_producer if item not in data_consumer]
    #         if difference:
    #             producer = KafkaProducer(bootstrap_servers='localhost:9092'
    #                                      , value_serializer=lambda v: json.dumps(v).encode('utf-8')
    #                                      )
    #
    #             for data in difference:
    #                 producer.send(data, "datdepzai")
    #                 print(difference)
    #                 producer.flush()
    #
    #         yield "-------validate pass ---------"
        # else:
        #     print("--------missing data---------")
        #     yield validate_kafka(data_producer,data_consumer)
    # for results in consumer(producer()):
    #     print(results)

    #
    # count_messages_producer, data_producer = next(producer())
    # print(count_messages_producer)
    # for i in data_producer:
    #     print(i)
    # print ("==============================================")
    # count_messages_consumer, data_consumer = next(consumer())
    # print(count_messages_consumer)
    # for i in data_consumer:
    #     print(i)
    # print ("==============================================")

    for abc in consumer(producer()):
        print(abc)




if __name__ == "__main__":
    main()


