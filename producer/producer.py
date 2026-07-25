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
    "R200": "Bridge",
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
    "R100": 0.08,
    "R200": 0.08,
    "R300": 0.06,
    "R400": 0.05,
    "R500": 0.04,
    "R600": 0.07,
    "R700": 0.05,
    "R800": 0.06
}

# ==========================================================
# Zones + Zone Type
# ==========================================================

road_zones = {

    "R100": [
        ("Nasr City", "Residential"),
        ("Maadi", "Residential"),
        ("Shubra", "Residential"),
        ("El Marg", "Residential"),
        ("Ain Shams", "Residential")
    ],

    "R200": [
        ("Downtown", "Commercial"),
        ("Dokki", "Commercial"),
        ("Mohandessin", "Commercial"),
        ("Zamalek", "Commercial"),
        ("Ramses", "Commercial")
    ],

    "R300": [
        ("Nasr City", "Residential"),
        ("Heliopolis", "Residential"),
        ("Airport", "Airport"),
        ("Ramses", "Commercial")
    ],

    "R400": [
        ("Maadi", "Residential"),
        ("Helwan", "Industrial"),
        ("New Cairo", "Residential")
    ],

    "R500": [
        ("6th October", "Residential"),
        ("Giza", "Residential")
    ],

    "R600": [
        ("Maadi", "Residential"),
        ("Downtown", "Commercial"),
        ("Zamalek", "Commercial"),
        ("Shubra", "Residential")
    ],

    "R700": [
        ("6th October", "Residential"),
        ("Mohandessin", "Commercial"),
        ("Dokki", "Commercial")
    ],

    "R800": [
        ("Nasr City", "Residential"),
        ("Heliopolis", "Residential"),
        ("New Cairo", "Residential")
    ]

}

# ==========================================================
# Weather by Season (Weighted)
# ==========================================================

weather_by_season = {

    "winter": (
        ["CLEAR", "CLOUDY", "RAIN", "FOG", "STORM"],
        [35, 30, 20, 10, 5]
    ),

    "spring": (
        ["CLEAR", "CLOUDY", "DUST", "WINDY", "RAIN"],
        [40, 20, 20, 15, 5]
    ),

    "summer": (
        ["CLEAR", "HOT", "DUST", "WINDY"],
        [55, 20, 15, 10]
    ),

    "autumn": (
        ["CLEAR", "CLOUDY", "RAIN", "FOG"],
        [40, 30, 20, 10]
    )

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

year_weights = [22, 34, 30, 14]

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

    month = random.randint(1, 7) if year == 2026 else random.randint(1, 12)

    while True:

        try:

            day = random.randint(
                1,
                28 if month == 2 else
                30 if month in [4, 6, 9, 11] else
                31
            )

            hour = random.choices(

                population=list(range(24)),

                weights=[
                    2,2,2,2,2,2,
                    3,
                    8,
                    9,
                    6,
                    5,
                    4,
                    4,
                    4,
                    4,
                    6,
                    9,
                    10,
                    9,
                    8,
                    6,
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

    if month in [12,1,2]:
        return "winter"

    elif month in [3,4,5]:
        return "spring"

    elif month in [6,7,8]:
        return "summer"

    return "autumn"


def generate_weather(dt):

    weather_list, weights = weather_by_season[get_season(dt.month)]

    return random.choices(
        weather_list,
        weights=weights,
        k=1
    )[0]


def is_peak_hour(dt):
    return (
        7 <= dt.hour <= 9 or
        16 <= dt.hour <= 19
    )


def is_weekend(dt):
    return dt.weekday() in [4,5]


def is_holiday(dt):
    return (dt.month, dt.day) in holidays


def has_accident():
    return random.random() < 0.08


def has_maintenance(road):
    return (
        random.random()
        <
        maintenance_probability[road]
    )# ==========================================================
# Congestion Simulator
# ==========================================================

def generate_congestion(
    road,
    dt,
    weather,
    accident,
    maintenance
):

    congestion = random.randint(1, 2)

    # Morning / Evening Peak
    if is_peak_hour(dt):
        congestion += random.randint(2, 3)

    # Weekend Night
    if is_weekend(dt) and 18 <= dt.hour <= 23:
        congestion += random.randint(1, 2)

    # Holidays
    if is_holiday(dt):
        congestion += random.randint(1, 2)

    # Weather Effect
    if weather in ["RAIN", "FOG"]:
        congestion += 1

    if weather == "DUST":
        congestion += 1

    if weather == "STORM":
        congestion += 2

    # Accident
    if accident:
        congestion += 2

    # Maintenance
    if maintenance:
        congestion += 1

    congestion = min(
        road_capacity[road],
        congestion
    )

    congestion = max(1, congestion)

    return congestion


# ==========================================================
# Speed Simulator
# ==========================================================

def generate_speed(
    road,
    congestion,
    weather,
    accident,
    maintenance
):

    limit = speed_limit[road]

    if congestion == 1:
        speed = random.randint(
            int(limit * 0.85),
            limit
        )

    elif congestion == 2:
        speed = random.randint(
            int(limit * 0.65),
            int(limit * 0.85)
        )

    elif congestion == 3:
        speed = random.randint(
            int(limit * 0.45),
            int(limit * 0.65)
        )

    elif congestion == 4:
        speed = random.randint(
            int(limit * 0.20),
            int(limit * 0.45)
        )

    else:
        speed = random.randint(
            5,
            int(limit * 0.20)
        )

    if weather == "RAIN":
        speed *= 0.85

    elif weather == "FOG":
        speed *= 0.75

    elif weather == "DUST":
        speed *= 0.80

    elif weather == "STORM":
        speed *= 0.55

    if accident:
        speed *= 0.55

    if maintenance:
        speed *= 0.80

    return max(5, int(speed))


# ==========================================================
# Traffic Risk
# ==========================================================

def calculate_risk(
    speed,
    congestion,
    weather,
    accident,
    maintenance
):

    score = 0

    if congestion >= 4:
        score += 3

    elif congestion == 3:
        score += 2

    elif congestion == 2:
        score += 1

    if speed < 20:
        score += 3

    elif speed < 40:
        score += 2

    elif speed < 60:
        score += 1

    if weather in ["RAIN", "FOG"]:
        score += 1

    if weather == "DUST":
        score += 1

    if weather == "STORM":
        score += 3

    if accident:
        score += 3

    if maintenance:
        score += 1

    if score >= 9:
        return "Critical"

    elif score >= 6:
        return "High"

    elif score >= 3:
        return "Medium"

    return "Low"


# ==========================================================
# Clean Event
# ==========================================================

def generate_clean_event():

    event_time = generate_random_datetime()

    road = choose_road()

    zone, zone_type = random.choice(
        road_zones[road]
    )

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

    risk = calculate_risk(
        speed,
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

        "road_name": roads[road],

        "road_type": road_type[road],

        "speed_limit": speed_limit[road],

        "city_zone": zone,

        "zone_type": zone_type,

        "speed": speed,

        "congestion_level": congestion,

        "weather": weather,

        "traffic_risk": risk,

        "accident": accident,

        "maintenance": maintenance,

        "event_time": event_time.isoformat()

    }


# ==========================================================
# Dirty Event
# ==========================================================

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

        base["speed"] = -25

    elif dirty_type == "extreme_speed":

        base["speed"] = 280

    elif dirty_type == "duplicate_vehicle":

        if vehicle_cache:
            base["vehicle_id"] = random.choice(vehicle_cache)

    elif dirty_type == "late_event":

        dt = datetime.fromisoformat(
            base["event_time"]
        )

        dt -= timedelta(
            minutes=random.randint(30,180)
        )

        base["event_time"] = dt.isoformat()

    elif dirty_type == "future_event":

        dt = datetime.now(pytz.utc)

        dt += timedelta(
            minutes=random.randint(30,180)
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


# ==========================================================
# Streaming Loop
# ==========================================================

print("=" * 60)
print("Traffic Producer Started")
print("=" * 60)

while True:

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

            f"Cong={event['congestion_level']} | "

            f"{event['traffic_risk']}"

        )

    producer.flush()

    time.sleep(
        random.uniform(0.3, 1.0)
    )