from kafka import KafkaConsumer

def consumer():

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



def main():
    # for count_consumer, data_consumer in consumer():
    #     print(f"-------consumer messeages = {count_consumer}")
    #     print(f"-------consumer messeages = {data_consumer}")
    while True:
        print("consumerrrrrrrrrrrrrrrrrrrrrrrrrrr")
if __name__ == "__main__":
    main()