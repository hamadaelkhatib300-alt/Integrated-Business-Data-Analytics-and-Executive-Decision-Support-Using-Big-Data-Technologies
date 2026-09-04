import json
import time
import pandas as pd
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TELCO_TOPIC = 'telco_data_topic'
RISK_TOPIC = 'risk_data_topic'

telco_path = '/home/bigdata/Desktop/WA_Fn-UseC_-Telco-Customer-Churn.csv'
risk_path = '/home/bigdata/Desktop/big4_financial_risk_compliance.csv'

print(" جاري قراءة ملفات CSV وضع البيانات في كافكا...")

try:
    df_telco = pd.read_csv(telco_path)
    for index, row in df_telco.iterrows():
        producer.send(TELCO_TOPIC, value=row.to_dict())
        if index % 500 == 0:
            print(f"تم ضخ {index} سطر إلى {TELCO_TOPIC}")

    df_risk = pd.read_csv(risk_path)
    for index, row in df_risk.iterrows():
        producer.send(RISK_TOPIC, value=row.to_dict())
        if index % 500 == 0:
            print(f"تم ضخ {index} سطر إلى {RISK_TOPIC}")

    producer.flush()
    print(" تم ضخ جميع البيانات بنجاح في كافكا!")

except Exception as e:
    print(f" حصل خطأ أثناء ضخ البيانات: {e}")