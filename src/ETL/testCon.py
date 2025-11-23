from kafka import KafkaConsumer

def consumer(tuplee):

    consumer = KafkaConsumer("datdepzai", group_id="group", bootstrap_servers=["localhost:9092"])
    total_message_consumer = 0
    running = True
    while running:
        msg_pack = consumer.poll(timeout_ms=500)
        data_consumer = []
        for tp, messages in msg_pack.items():
            # print (messages)
            for message in messages:
                # print(message.value.decode('utf-8'))
                data = message.value.decode('utf-8')
                data_consumer.append(data)
                total_message_consumer += 1
                # print(f"-------------------{total_message_consumer}-----------------")
                yield total_message_consumer, data_consumer

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


def main():
    # for count_consumer, data_consumer in consumer():
    #     print(f"-------consumer messeages = {count_consumer}")
    #     print(f"-------consumer messeages = {data_consumer}")
    while True:
        print("consumerrrrrrrrrrrrrrrrrrrrrrrrrrr")
if __name__ == "__main__":
    main()