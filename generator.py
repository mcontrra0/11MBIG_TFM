import logging
import time;
import datetime;
import random
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STRING = "CONNECTION_STRING_CREDENTIALS"
EVENT_HUB_NAME = "EVENT_HUB_NAME"

def generateData():
    try:
        if not CONNECTION_STRING:
            raise ValueError("No EventHubs URL supplied.")
        producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)
        event_data_batch = producer.create_batch()
        try:
            start_time = time.time()
            df = pd.read_csv("..data-diabetes-monitoring.csv") 
            first = True
            while True:
                sensor = pd.DataFrame(columns=['ds_id', 'patient', 'gender', 'age', 'type','blood_glucose', 'heart_rate'])
                for i in df.index:
                    patient = df['patient'][i]
                    gender = df['gender'][i]
                    age = df['age'][i]
                    type = df['type'][i]

                    if first == True:
                        blood_glucose = df['blood_glucose'][i]
                        heart_rate = df['heart_rate'][i]
                        ds_id = df['ds_id'][i]
                    else:
                        blood_glucose = generate(df['blood_glucose'][i])
                        heart_rate = generate(df['heart_rate'][i])

                    sensor = sensor.append({'ds_id': ds_id, 'patient': patient, 'gender': gender ,'age': age, 'type': type, 'blood_glucose': blood_glucose, 'heart_rate': heart_rate}, ignore_index=True)

                first = False
                event_data_batch.add(EventData(sensor.to_json(orient="records")))
                producer.send_batch(event_data_batch)
                time.sleep(3)
                ds_id = ds_id + 1

        except:
            raise
        finally:
            end_time = time.time()
            producer.close()
            run_time = end_time - start_time
    except KeyboardInterrupt:
        pass
generateData()
