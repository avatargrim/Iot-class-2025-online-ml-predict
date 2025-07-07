import os
import logging
import joblib  # For saving/loading the model
import json
from datetime import datetime, timezone
from river import tree, preprocessing, metrics
from quixstreams import Application
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Load environment variables from .env file
load_dotenv(".env")

# Configure logging
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Load configurations
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "event-frames-model")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "fan-speed-prediction")
MODEL_LOCATION = os.getenv("MODEL_LOCATION", "/app/fan_speed_model.pkl")

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

# Load or initialize the online model
if os.path.exists(MODEL_LOCATION):
    model = joblib.load(MODEL_LOCATION)
    logging.info(f"✅ Loaded model from {MODEL_LOCATION}")
else:
    model = preprocessing.StandardScaler() | tree.HoeffdingTreeClassifier()
    logging.info(f"📦 Initialized new model")

# Metrics for evaluation
metric_acc = metrics.Accuracy()
metric_mae = metrics.MAE()

# Record counter for triggering model save
counter = 0

# Setup QuixStreams application
try:
    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="INFO",
        auto_offset_reset="earliest",
        state_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "state"),
        consumer_group="predict-from-kafka-online"
    )
    input_topic = app.topic(KAFKA_INPUT_TOPIC, value_deserializer="json")
    producer = app.get_producer()
    output_topic = app.topic(KAFKA_OUTPUT_TOPIC, value_serializer="json")
    logging.info(f"✅ Kafka topics configured: input='{KAFKA_INPUT_TOPIC}', output='{KAFKA_OUTPUT_TOPIC}'")
except Exception as e:
    logging.error(f"❌ Failed to setup QuixStreams application or topics: {e}")
    exit(1)

# Handler function to process each message from Kafka
def handle_message(data):
    global counter
    try:
        sensor_name = data.get("name", "")
        payload = data.get("payload", {})
        x = {
            "temperature": payload["temperature"],
            "humidity": payload["humidity"],
            "pressure": payload["pressure"]
        }
        
        # Make prediction before learning
        prediction = model.predict_one(x)

        # Convert timestamp to datetime (UTC)
        timestamp = datetime.fromtimestamp(
            payload.get("timestamp", datetime.now().timestamp() * 1000) / 1000.0,
            tz=timezone.utc
        )

        if sensor_name == "iot_sensor_0":
            actual_fan_speed = payload.get("fan_speed", None)
            if actual_fan_speed is not None:
                # Update the model with true value
                model.learn_one(x, actual_fan_speed)

                # Update metrics
                metric_acc.update(actual_fan_speed, prediction)
                metric_mae.update(actual_fan_speed, prediction)

                logging.info(f"🔍[Model Sensor] {sensor_name} Predict={prediction}, Target={actual_fan_speed}, Acc={metric_acc.get():.3f}, MAE={metric_mae.get():.3f}")

                # Save the model every 10 records
                counter += 1
                if counter % 10 == 0:
                    joblib.dump(model, MODEL_LOCATION)
                    logging.info(f"💾 Model saved after {counter} records to {MODEL_LOCATION}")
                    counter = 0
            else:
                logging.warning("[Model Sensor] fan_speed not found in payload")
                logging.warning(f"📥 First learn only: Target={actual_fan_speed}")

            # Build output payload including actual and predicted values
            result_payload = {
                "temperature": payload["temperature"],
                "humidity": payload["humidity"],
                "pressure": payload["pressure"],
                "fan_speed": int(actual_fan_speed) if actual_fan_speed is not None else None,
                "fan_speed_predicted": int(prediction),
                "timestamp": int(timestamp.timestamp() * 1000),
                "date": timestamp.isoformat()
            }

        else:
            # For other sensors (only prediction)
            result_payload = {
                "temperature": payload["temperature"],
                "humidity": payload["humidity"],
                "pressure": payload["pressure"],
                "fan_speed_predicted": int(prediction),
                "timestamp": int(timestamp.timestamp() * 1000),
                "date": timestamp.isoformat()
            }

        # Compose final Kafka message
        result = {
            "id": data.get("id", ""),
            "name": sensor_name,
            "place_id": data.get("place_id", ""),
            "payload": result_payload
        }

        new_payload = json.dumps(result).encode('utf-8')

        # Send result to Kafka output topic
        producer.produce(
            topic=output_topic.name,
            key=sensor_name,
            value=new_payload,
            timestamp=int(timestamp.timestamp() * 1000)
        )
        logging.info(f"[📤] Sent prediction to Kafka topic: {KAFKA_OUTPUT_TOPIC}, payload: {json.dumps(result)}")

        # Write only prediction to InfluxDB
        point = (
            Point("fan_speed_prediction")
            .tag("sensor_id", data.get("id", "unknown"))
            .tag("place_id", data.get("place_id", "unknown"))
            .tag("name", sensor_name)
            .field("fan_speed_predicted", int(prediction))
            .time(timestamp)
        )
        influx_writer.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info("[📊] Wrote prediction to InfluxDB")

    except KeyError as e:
        logging.error(f"❌ Missing key in payload: {e}")
    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")

# Apply the message handler to Kafka dataframe stream
sdf = app.dataframe(input_topic)
sdf = sdf.apply(handle_message)

# Start application
logging.info(f"Connecting to ...{KAFKA_BROKER}")
logging.info(f"🚀 Listening to Kafka topic: {KAFKA_INPUT_TOPIC}")
app.run()
