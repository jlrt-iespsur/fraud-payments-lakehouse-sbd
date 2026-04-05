#!/usr/bin/env python3

# Generador secuencial de pagos para Kafka

import argparse
import json
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


# Configuración base de los inserts
COUNTRIES = ["ES", "PT", "FR", "IT", "DE", "GB", "US", "MX"]
MCC_CODES = ["5411", "5732", "5812", "4111", "4900", "5999", "5651", "5942"]
CURRENCIES_BY_COUNTRY = {
    "ES": "EUR",
    "PT": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "DE": "EUR",
    "GB": "GBP",
    "US": "USD",
    "MX": "MXN",
}


# Parámetros CLI para hacer pruebas con distintas configuraciones
parser = argparse.ArgumentParser(description="Generador de pagos para Kafka")
parser.add_argument("--bootstrap-servers", default="kafka:9092")
parser.add_argument("--topic", default="payments")
parser.add_argument("--events", type=int, default=2000)
parser.add_argument("--sleep-ms", type=int, default=120)
parser.add_argument("--profiles", type=int, default=250)
parser.add_argument("--seed", type=int, default=42)
args = parser.parse_args()


# Geeramos los datos iniciales
random.seed(args.seed)

profiles = []
for idx in range(args.profiles):
    country = random.choice(COUNTRIES[:5])
    profiles.append(
        {
            "customer_id": f"cust-{idx:05d}",
            "card_id": f"card-{idx:05d}",
            "home_country": country,
            "default_device_id": f"device-{random.randint(1, max(60, args.profiles // 3)):04d}",
        }
    )

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap_servers,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    linger_ms=50,
    acks="all",
)

recent_declines = deque(maxlen=2000)
suspicious_burst_remaining = 0
burst_profile = None


# Repetimos hasta completar los eventos configurados
for _ in range(args.events):
    selector = random.random()

    # Elegimos perfil y escenario.
    scenario = "normal"
    profile = random.choice(profiles)
    base_declined = None

    if suspicious_burst_remaining > 0 and burst_profile is not None:
        scenario = "suspicious"
        profile = burst_profile
        suspicious_burst_remaining -= 1
    elif selector < 0.74 or not recent_declines:
        scenario = "normal"
    elif selector < 0.88:
        scenario = "retry"
        base_declined = random.choice(list(recent_declines))
        profile = {
            "customer_id": base_declined["customer_id"],
            "card_id": base_declined["card_id"],
            "home_country": base_declined["country"],
            "default_device_id": base_declined["device_id"],
        }
    else:
        scenario = "suspicious"
        burst_profile = random.choice(profiles)
        profile = burst_profile
        suspicious_burst_remaining = random.randint(2, 6) - 1

    # Valores según el escenario
    if scenario == "normal":
        merchant_country = profile["home_country"] if random.random() < 0.9 else random.choice(COUNTRIES[:6])
        merchant_id = f"merchant-{random.randint(1, 120):04d}"
        amount = round(random.triangular(3, 350, 45), 2)
        status = "approved" if random.random() < 0.93 else "declined"
        mcc = random.choice(MCC_CODES)
        device_id = profile["default_device_id"]
        event_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    elif scenario == "retry":
        merchant_country = base_declined["country"]
        merchant_id = base_declined["merchant_id"]
        amount = float(base_declined["amount"])
        status = "approved" if random.random() < 0.62 else "declined"
        mcc = base_declined["mcc"]
        device_id = base_declined["device_id"]
        event_time = (datetime.now(timezone.utc) + timedelta(seconds=random.randint(4, 90))).replace(
            microsecond=0
        ).isoformat()
    else:
        merchant_country = random.choice(COUNTRIES)
        merchant_id = f"merchant-{random.randint(121, 190):04d}"
        amount = round(random.uniform(700, 2800), 2)
        status = "approved" if random.random() < 0.45 else "declined"
        mcc = random.choice(MCC_CODES)
        device_id = f"device-{random.randint(1, 20):04d}"
        event_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # Generamos el evento final con los campos requeridos
    event = {
        "event_time": event_time,
        "payment_id": f"pay-{uuid.uuid4().hex[:20]}",
        "customer_id": profile["customer_id"],
        "card_id": profile["card_id"],
        "merchant_id": merchant_id,
        "device_id": device_id,
        "ip": ".".join(str(random.randint(1, 254)) for _ in range(4)),
        "country": merchant_country,
        "amount": amount,
        "currency": CURRENCIES_BY_COUNTRY.get(merchant_country, "EUR"),
        "status": status,
        "mcc": mcc,
    }

    # y lo enviamos
    producer.send(args.topic, key=event["card_id"].encode("utf-8"), value=event)

    if event["status"] == "declined":
        recent_declines.append(event)

    time.sleep(args.sleep_ms / 1000.0)


# Terminamos bien el producer y que se manden
# los que Kafka tenga aún encolados
producer.flush()
producer.close()
