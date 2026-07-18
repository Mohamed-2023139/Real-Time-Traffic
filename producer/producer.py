from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime, timedelta
import pytz

fake = Faker()

# ==========================================================
# Kafka Producer
# ==========================================================

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==========================================================
# Roads
# ==========================================================

roads = {
    "R100": "Ring Road",
    "R200": "6th October Bridge",
    "R300": "Salah Salem Road",
    "R400": "Autostrad Road",
    "R500": "Cairo Alexandria Desert Road",
    "R600": "Nile Corniche",
    "R700": "26th of July Corridor",
    "R800": "El Nasr Road"
}

# ==========================================================
# Road Types
# ==========================================================

road_type = {
    "R100": "Highway",
    "R200": "City Road",
    "R300": "Main Road",
    "R400": "Express Road",
    "R500": "Highway",
    "R600": "Corniche",
    "R700": "Express Road",
    "R800": "City Road"
}

# ==========================================================
# Speed Limits
# ==========================================================

speed_limit = {
    "R100": 100,
    "R200": 80,
    "R300": 90,
    "R400": 90,
    "R500": 120,
    "R600": 70,
    "R700": 100,
    "R800": 80
}

# ==========================================================
# Road Capacity
# ==========================================================

road_capacity = {
    "R100": 5,
    "R200": 5,
    "R300": 4,
    "R400": 4,
    "R500": 3,
    "R600": 3,
    "R700": 4,
    "R800": 4
}

# ==========================================================
# Maintenance Probability
# ==========================================================

maintenance_probability = {
    "R100": 0.04,
    "R200": 0.05,
    "R300": 0.03,
    "R400": 0.03,
    "R500": 0.02,
    "R600": 0.04,
    "R700": 0.03,
    "R800": 0.04
}

# ==========================================================
# Zones
# ==========================================================

road_zones = {
    "R100": [
        "Nasr City",
        "Maadi",
        "Shubra",
        "El Marg",
        "Ain Shams"
    ],

    "R200": [
        "Downtown",
        "Dokki",
        "Mohandessin",
        "Zamalek",
        "Ramses"
    ],

    "R300": [
        "Nasr City",
        "Heliopolis",
        "Airport",
        "Ramses"
    ],

    "R400": [
        "Maadi",
        "Helwan",
        "New Cairo"
    ],

    "R500": [
        "6th October",
        "Giza"
    ],

    "R600": [
        "Maadi",
        "Downtown",
        "Zamalek",
        "Shubra"
    ],

    "R700": [
        "6th October",
        "Mohandessin",
        "Dokki"
    ],

    "R800": [
        "Nasr City",
        "Heliopolis",
        "New Cairo"
    ]
}

# ==========================================================
# Weather By Season
# ==========================================================

weather_by_season = {
    "winter": [
        "CLEAR",
        "CLOUDY",
        "RAIN",
        "FOG"
    ],

    "spring": [
        "CLEAR",
        "CLOUDY",
        "DUST",
        "WINDY"
    ],

    "summer": [
        "CLEAR",
        "HOT",
        "DUST",
        "WINDY"
    ],

    "autumn": [
        "CLEAR",
        "CLOUDY",
        "RAIN",
        "FOG"
    ]
}

# ==========================================================
# Balanced Road Distribution
# ==========================================================

road_weights = {
    "R100": 13,
    "R200": 13,
    "R300": 12,
    "R400": 12,
    "R500": 12,
    "R600": 13,
    "R700": 13,
    "R800": 12
}

# ==========================================================
# Years Distribution
# ==========================================================

years = [2023, 2024, 2025, 2026]

# 2024 أكبر قليلاً ثم 2025 ثم 2023 ثم 2026
year_weights = [
    24,
    34,
    30,
    12
]

# ==========================================================
# Egyptian Holidays
# ==========================================================

holidays = {
    (1, 1),
    (1, 7),
    (4, 25),
    (5, 1),
    (6, 30),
    (7, 23),
    (10, 6)
}

vehicle_cache = []

utc = pytz.utc
cairo = pytz.timezone("Africa/Cairo")

# ==========================================================
# Random Historical Date
# ==========================================================

def generate_random_datetime():
    year = random.choices(
        years,
        weights=year_weights,
        k=1
    )[0]

    if year == 2026:
        month = random.randint(1, 7)
    else:
        month = random.randint(1, 12)

    while True:
        try:
            if month == 2:
                day = random.randint(1, 28)
            elif month in [4, 6, 9, 11]:
                day = random.randint(1, 30)
            else:
                day = random.randint(1, 31)

            # توزيع متوازن للساعات
            hour = random.choices(
                population=list(range(24)),
                weights=[
                    2,2,2,2,2,2,
                    3,
                    7,
                    8,
                    6,
                    5,
                    4,
                    4,
                    4,
                    4,
                    5,
                    8,
                    9,
                    8,
                    7,
                    5,
                    4,
                    3,
                    2
                ],
                k=1
            )[0]

            minute = random.randint(0,59)
            second = random.randint(0,59)

            dt = cairo.localize(
                datetime(
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second
                )
            )

            return dt.astimezone(utc)

        except:
            continue

# ==========================================================
# Helper Functions
# ==========================================================

def choose_road():
    return random.choices(
        list(road_weights.keys()),
        weights=list(road_weights.values()),
        k=1
    )[0]


def get_season(month):
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    return "autumn"


def generate_weather(dt):
    weather = random.choice(
        weather_by_season[get_season(dt.month)]
    )

    # أحداث نادرة
    if random.random() < 0.02:
        weather = "STORM"

    return weather


def is_peak_hour(dt):
    return (
        7 <= dt.hour <= 9
        or
        16 <= dt.hour <= 19
    )


def is_weekend(dt):
    return dt.weekday() in [4, 5]


def is_holiday(dt):
    return (dt.month, dt.day) in holidays


def has_accident():
    return random.random() < 0.02


def has_maintenance(road):
    return (
        random.random()
        <
        maintenance_probability[road]
    )

# ==========================================================
# Congestion & Speed Simulators (The Missing Functions)
# ==========================================================

def generate_congestion(road, dt, weather, accident, maintenance):
    """
    حساب مستوى الازدحام بناءً على الوقت، الطقس، الحوادث والصيانة.
    """
    base_congestion = random.randint(1, 2)
    
    # زيادة الازدحام في أوقات الذروة في الأيام العادية
    if is_peak_hour(dt) and not is_weekend(dt) and not is_holiday(dt):
        base_congestion += random.randint(2, 3)
    elif is_weekend(dt) and 18 <= dt.hour <= 22:
        base_congestion += 1

    if accident:
        base_congestion += 2
    if maintenance:
        base_congestion += 1

    if weather in ["RAIN", "FOG", "STORM", "DUST"]:
        base_congestion += 1

    max_cap = road_capacity.get(road, 5)
    return min(max_cap, max(1, base_congestion))


def generate_speed(road, congestion, weather, accident, maintenance):
    """
    توليد سرعة منطقية للسيارة بناءً على حد السرعة والازدحام والطقس.
    """
    limit = speed_limit.get(road, 90)
    
    # تأثير الازدحام على السرعة
    congestion_modifiers = {
        1: random.uniform(0.9, 1.1),
        2: random.uniform(0.7, 0.9),
        3: random.uniform(0.4, 0.6),
        4: random.uniform(0.2, 0.4),
        5: random.uniform(0.05, 0.15)
    }
    
    modifier = congestion_modifiers.get(congestion, 0.5)
    target_speed = limit * modifier

    # تأثير الطقس السيء
    if weather == "STORM":
        target_speed *= 0.5
    elif weather in ["RAIN", "FOG"]:
        target_speed *= 0.7
    elif weather == "DUST":
        target_speed *= 0.85

    # تأثير الحوادث والصيانة
    if accident:
        target_speed *= 0.6
    if maintenance:
        target_speed *= 0.8

    return int(max(5, target_speed))

# =====================================================
# Clean Event Generator
# =====================================================

def generate_clean_event():
    event_time = generate_random_datetime()
    road = choose_road()
    zone = random.choice(road_zones[road])
    weather = generate_weather(event_time)
    accident = has_accident()
    maintenance = has_maintenance(road)

    congestion = generate_congestion(
        road,
        event_time,
        weather,
        accident,
        maintenance
    )

    speed = generate_speed(
        road,
        congestion,
        weather,
        accident,
        maintenance
    )

    vehicle_id = fake.uuid4()
    vehicle_cache.append(vehicle_id)

    return {
        "vehicle_id": vehicle_id,
        "road_id": road,
        "city_zone": zone,
        "speed": speed,
        "congestion_level": congestion,
        "weather": weather,
        "event_time": event_time.isoformat()
    }


# =====================================================
# Dirty Event Generator
# =====================================================

def generate_dirty_event():
    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json"
    ])

    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed"] = None

    elif dirty_type == "negative_speed":
        base["speed"] = -50

    elif dirty_type == "extreme_speed":
        base["speed"] = 350

    elif dirty_type == "duplicate_vehicle" and vehicle_cache:
        base["vehicle_id"] = random.choice(vehicle_cache)

    elif dirty_type == "late_event":
        dt = datetime.fromisoformat(
            base["event_time"]
        )
        dt -= timedelta(
            minutes=random.randint(30, 180)
        )
        base["event_time"] = dt.isoformat()

    elif dirty_type == "future_event":
        dt = datetime.now(pytz.utc)
        dt += timedelta(
            minutes=random.randint(30, 180)
        )
        base["event_time"] = dt.isoformat()

    elif dirty_type == "wrong_datatype":
        base["speed"] = "FAST"

    elif dirty_type == "schema_drift":
        base["road_condition"] = random.choice([
            "GOOD",
            "BAD",
            "UNDER_CONSTRUCTION"
        ])

    elif dirty_type == "corrupt_json":
        return "###CORRUPTED_EVENT###"

    return base


# =====================================================
# Streaming Loop
# =====================================================

print("=" * 60)
print("Traffic Producer Started...")
print("=" * 60)

while True:
    # 85% Clean
    # 15% Dirty
    if random.random() < 0.85:
        event = generate_clean_event()
    else:
        event = generate_dirty_event()

    if isinstance(event, str):
        producer.send(
            "traffic-topic",
            value={"raw": event}
        )
        print("CORRUPTED EVENT")
    else:
        producer.send(
            "traffic-topic",
            value=event
        )
        print(
            f"[{event['event_time'][:19]}] "
            f"{event['road_id']} | "
            f"{event['city_zone']} | "
            f"{event['weather']} | "
            f"{event['speed']} km/h | "
            f"Cong={event['congestion_level']}"
        )

    producer.flush()

    # معدل وصول الأحداث
    time.sleep(
        random.uniform(0.4, 1.2)
    )