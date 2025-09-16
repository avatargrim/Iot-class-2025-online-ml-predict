import os
import logging
import joblib
import json
from datetime import datetime, timezone
from river import preprocessing, metrics, ensemble
from quixstreams import Application
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Load environment variables
load_dotenv(".env")

# Configure logging
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Load configs
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "6510301032_AQ")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "aqi-prediction")
MODEL_LOCATION = os.getenv("MODEL_LOCATION", "/app/aqi_model_forest.pkl")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# Initialize InfluxDB client
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    influx_writer = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1))
    logging.info("✅ InfluxDB client initialized successfully")
except Exception as e:
    logging.error(f"❌ Failed to initialize InfluxDB client: {e}")
    exit(1)

# Initialize river model (Adaptive Random Forest Regressor with tuning)
if os.path.exists(MODEL_LOCATION):
    model = joblib.load(MODEL_LOCATION)
    logging.info(f"✅ Loaded AdaptiveRandomForestRegressor model from {MODEL_LOCATION}")
else:
    model = preprocessing.StandardScaler() | ensemble.AdaptiveRandomForestRegressor(
        n_models=30,       # จำนวนต้นไม้ (มากขึ้นแม่นยำขึ้น)
        max_depth=20,      # จำกัดความลึกปานกลาง ลด overfitting
        max_features=3,    # สุ่มฟีเจอร์ช่วยลด correlation
        seed=42            # reproducible
    )
    logging.info(f"📦 Initialized new AdaptiveRandomForestRegressor model")

# Metrics
metric_mae = metrics.MAE()
metric_rmse = metrics.RMSE()
metric_r2 = metrics.R2()

counter = 0  # record counter

# Setup QuixStreams app
try:
    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="INFO",
        auto_offset_reset="earliest",
        state_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "state"),
        consumer_group="predict-aqi-online"
    )
    input_topic = app.topic(KAFKA_INPUT_TOPIC, value_deserializer="json")
    producer = app.get_producer()
    output_topic = app.topic(KAFKA_OUTPUT_TOPIC, value_serializer="json")
    logging.info(f"✅ Kafka topics configured: input='{KAFKA_INPUT_TOPIC}', output='{KAFKA_OUTPUT_TOPIC}'")
except Exception as e:
    logging.error(f"❌ Failed to setup QuixStreams application or topics: {e}")
    exit(1)

# Message handler
def handle_message(data):
    global counter
    try:
        sensor_name = data.get("name", "")
        payload = data.get("payload", {})

        # Features
        x = {
            "CO": payload["CO"],
            "NO2": payload["NO2"],
            "SO2": payload["SO2"],
            "O3": payload["O3"],
            "PM2_5": payload["PM2.5"],
            "PM10": payload["PM10"]
        }

        # Prediction (river forest)
        prediction = model.predict_one(x)

        # Timestamp
        timestamp = datetime.fromtimestamp(
            payload.get("timestamp", datetime.now().timestamp() * 1000) / 1000.0,
            tz=timezone.utc
        )

        actual_aqi = payload.get("AQI", None)

        if actual_aqi is not None:
            # Online learning
            model.learn_one(x, actual_aqi)
            # Update metrics
            metric_mae.update(actual_aqi, prediction)
            metric_rmse.update(actual_aqi, prediction)
            metric_r2.update(actual_aqi, prediction)
            logging.info(
                f"🌲 Predict={prediction:.2f}, Target={actual_aqi}, "
                f"MAE={metric_mae.get():.3f}, RMSE={metric_rmse.get():.3f}, R²={metric_r2.get():.3f}"
            )

            # Save model every 50 records
            counter += 1
            if counter % 50 == 0:
                joblib.dump(model, MODEL_LOCATION)
                logging.info(f"💾 Forest model saved after {counter} records to {MODEL_LOCATION}")
                counter = 0

        # Build output payload
        result_payload = {
            "CO": payload["CO"],
            "NO2": payload["NO2"],
            "SO2": payload["SO2"],
            "O3": payload["O3"],
            "PM2_5": payload["PM2.5"],
            "PM10": payload["PM10"],
            "AQI": int(actual_aqi) if actual_aqi is not None else None,
            "AQI_predicted": float(prediction) if prediction is not None else None,
            "timestamp": int(timestamp.timestamp() * 1000),
            "date": timestamp.isoformat()
        }

        result = {
            "id": data.get("id", ""),
            "name": sensor_name,
            "place_id": data.get("place_id", ""),
            "payload": result_payload
        }

        new_payload = json.dumps(result).encode('utf-8')

        # Send to Kafka output
        producer.produce(
            topic=output_topic.name,
            key=sensor_name,
            value=new_payload,
            timestamp=int(timestamp.timestamp() * 1000)
        )
        logging.info(f"[📤] Sent prediction to Kafka topic: {KAFKA_OUTPUT_TOPIC}, payload: {json.dumps(result)}")

        # Write prediction to InfluxDB
        if prediction is not None:
            point = (
                Point("aqi_prediction")
                .tag("sensor_id", data.get("id", "unknown"))
                .tag("place_id", data.get("place_id", "unknown"))
                .tag("name", sensor_name)
                .field("AQI_predicted", float(prediction))
                .time(timestamp)
            )
            influx_writer.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            logging.info("[📊] Wrote prediction to InfluxDB")

    except KeyError as e:
        logging.error(f"❌ Missing key in payload: {e}")
    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")

# Apply handler
sdf = app.dataframe(input_topic)
sdf = sdf.apply(handle_message)

# Run app
logging.info(f"Connecting to ...{KAFKA_BROKER}")
logging.info(f"🚀 Listening to Kafka topic: {KAFKA_INPUT_TOPIC}")
app.run()
