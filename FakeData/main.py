import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime
fake = Faker()

def generate_company_table():
    return {
        "ID": random.choice(['001', '002', '003', '004', '005', '006','007','008','009']),
        "MEMONIC_LP": random.randint(1, 999),
        "COMPANY_NAME": fake.company(),
        "CIF_ID":random.randint(1,7),
        "CIC":random.randint(1,3),
        "BRANCH_CODE":f"{random.randint(1, 9):03}",
        "BRANCH_NAME": fake.company_suffix(),
        "PARENT_ID": f"{random.randint(1, 9):03}",
        "WORKING_BALANCE": random.randint(100000, 1000000)
    }



def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")
def main():
    topic_company = 'fake_data_company'
    producer= SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 60:
        try:
            fake_data_company = generate_company_table()
            print(fake_data_company)

            producer.produce(topic_company,
                             key=fake_data_company['ID'],
                             value=json.dumps(fake_data_company),
                             on_delivery=delivery_report
                             )
            producer.poll(0)
            time.sleep(5)
            
            
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)
     
if __name__ == "__main__":
    main()