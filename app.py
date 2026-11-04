
import streamlit as st
import paho.mqtt.client as mqtt
import json
import ssl
import time
import threading
import pandas as pd
import plotly.express as px
from collections import deque
from datetime import datetime, timezone

# =========================================================
# POST-HARVEST MONITORING AND CONTROL DASHBOARD
# ESP8266 + MQTT + STREAMLIT
# =========================================================

st.set_page_config(
    page_title="Post-Harvest Monitoring System",
    page_icon="🌾",
    layout="wide"
)

st.markdown("""
<style>
.stApp {
    background: #0e1117;
    color: #f5f5f5;
}
[data-testid="stSidebar"] {
    background: #151b24;
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# MQTT CONFIGURATION
# =========================================================

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 8883
MQTT_KEEPALIVE = 60

try:
    TOPIC_KEY = st.secrets["mqtt"]["topic_key"]
except Exception:
    TOPIC_KEY = ""

BASE_TOPIC = f"phms/{TOPIC_KEY}"
TELEMETRY_TOPIC = f"{BASE_TOPIC}/telemetry"
COMMAND_TOPIC = f"{BASE_TOPIC}/commands"
STATUS_TOPIC = f"{BASE_TOPIC}/status"

# =========================================================
# SHARED MQTT STATE
# =========================================================

@st.cache_resource
def get_shared_state():
    return {
        "lock": threading.Lock(),
        "latest": None,
        "history": deque(maxlen=5000),
        "last_received": None,
        "connected": False,
        "mqtt_error": None,
        "command_result": None
    }


state = get_shared_state()


# =========================================================
# MQTT CALLBACKS
# =========================================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        with state["lock"]:
            state["connected"] = True
            state["mqtt_error"] = None

        client.subscribe(TELEMETRY_TOPIC)
        client.subscribe(STATUS_TOPIC)

    else:
        with state["lock"]:
            state["connected"] = False
            state["mqtt_error"] = str(reason_code)


def on_disconnect(client, userdata, disconnect_flags,
                  reason_code, properties):
    with state["lock"]:
        state["connected"] = False


def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))

        if message.topic == TELEMETRY_TOPIC:
            received_time = datetime.now(timezone.utc)

            payload["_received_at"] = received_time.isoformat()

            with state["lock"]:
                state["latest"] = payload
                state["last_received"] = received_time
                state["history"].append(payload)

        elif message.topic == STATUS_TOPIC:
            with state["lock"]:
                state["command_result"] = payload

    except Exception as error:
        with state["lock"]:
            state["mqtt_error"] = str(error)


# =========================================================
# MQTT CONNECTION
# =========================================================

@st.cache_resource
def start_mqtt():

    if not TOPIC_KEY or len(TOPIC_KEY) < 20:
        return None

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"phms_dashboard_{int(time.time())}",
        protocol=mqtt.MQTTv311
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.tls_set(
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT
    )

    client.reconnect_delay_set(
        min_delay=2,
        max_delay=30
    )

    try:
        client.connect_async(
            MQTT_BROKER,
            MQTT_PORT,
            MQTT_KEEPALIVE
        )

        client.loop_start()

        return client

    except Exception as error:
        with state["lock"]:
            state["mqtt_error"] = str(error)

        return None


mqtt_client = start_mqtt()


# =========================================================
# COMMAND FUNCTION
# =========================================================

def send_command(buzzer, fan, relay):

    if mqtt_client is None:
        raise RuntimeError("MQTT client is not configured.")

    if not mqtt_client.is_connected():
        raise RuntimeError("Dashboard is not connected to MQTT broker.")

    command = {
        "buzzer": buzzer,
        "fan": fan,
        "relay": relay,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    result = mqtt_client.publish(
        COMMAND_TOPIC,
        json.dumps(command),
        qos=1,
        retain=True
    )

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("Could not publish the command.")

    result.wait_for_publish(timeout=5)

    if not result.is_published():
        raise RuntimeError("Command publishing timed out.")

    return command


# =========================================================
# DATA FUNCTIONS
# =========================================================

def get_data():

    with state["lock"]:
        latest = dict(state["latest"]) if state["latest"] else None
        history = list(state["history"])
        last_received = state["last_received"]
        connected = state["connected"]
        error = state["mqtt_error"]

    return latest, history, last_received, connected, error


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def determine_connection(last_received):

    if last_received is None:
        return False

    age = (
        datetime.now(timezone.utc) - last_received
    ).total_seconds()

    return age <= 15


def build_history_dataframe(records):

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "_received_at" in df.columns:
        df["Time"] = pd.to_datetime(
            df["_received_at"],
            errors="coerce",
            utc=True
        )

    for column in [
        "temperature",
        "humidity",
        "soil_moisture_percent",
        "soil_moisture_raw"
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    return df


# =========================================================
# SIDEBAR
# =========================================================

st.sidebar.title("🌾 Post-Harvest System")

page = st.sidebar.radio(
    "Navigation",
    [
        "Executive Overview",
        "Live Sensor Monitoring",
        "Environmental Risk",
        "Historical Analytics",
        "Alerts & Recommendations",
        "Remote Device Control",
        "System Information"
    ]
)

st.sidebar.divider()

st.sidebar.caption("Communication: MQTT over TLS")
st.sidebar.caption("Broker: broker.emqx.io")
st.sidebar.caption("Refresh: 3 seconds")

# =========================================================
# MQTT CONFIGURATION CHECK
# =========================================================

if not TOPIC_KEY or len(TOPIC_KEY) < 20:

    st.error(
        "MQTT topic key is missing or too short. "
        "Configure it in Streamlit Secrets."
    )

    st.code("""
[mqtt]
topic_key = "YOUR_LONG_RANDOM_SHARED_TOPIC_KEY"
""", language="toml")

    st.stop()


# =========================================================
# READ DATA
# =========================================================

latest, records, last_received, broker_connected, mqtt_error = get_data()

device_online = determine_connection(last_received)

history_df = build_history_dataframe(records)

temperature = number(
    latest.get("temperature") if latest else None
)

humidity = number(
    latest.get("humidity") if latest else None
)

soil = number(
    latest.get("soil_moisture_percent") if latest else None
)

soil_raw = number(
    latest.get("soil_moisture_raw") if latest else None
)

risk = (
    latest.get("environmental_risk", "UNKNOWN")
    if latest else "UNKNOWN"
)

recommendation = (
    latest.get("recommendation", "No recommendation")
    if latest else "No recommendation"
)

# =========================================================
# HEADER
# =========================================================

st.title("🌾 Post-Harvest Loss Monitoring Dashboard")

st.caption(
    "Real-time environmental monitoring and remote hardware control."
)

col1, col2 = st.columns(2)

with col1:
    if broker_connected:
        st.success("MQTT broker connected")
    else:
        st.error("MQTT broker disconnected")

with col2:
    if device_online:
        st.success("ESP8266 device ONLINE")
    else:
        st.warning("ESP8266 device OFFLINE / NO RECENT DATA")

if mqtt_error:
    st.caption(f"MQTT diagnostic: {mqtt_error}")

# =========================================================
# PAGE: EXECUTIVE OVERVIEW
# =========================================================

if page == "Executive Overview":

    st.subheader("Executive Overview")

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Temperature",
        f"{temperature:.1f} °C" if temperature is not None else "N/A"
    )

    c2.metric(
        "Humidity",
        f"{humidity:.1f} %" if humidity is not None else "N/A"
    )

    c3.metric(
        "Soil Moisture",
        f"{soil:.1f} %" if soil is not None else "N/A"
    )

    c4.metric("Environmental Risk", str(risk))

    st.divider()

    left, right = st.columns(2)

    with left:
        st.markdown("### Environmental Condition")

        st.write("**Risk:**", risk)
        st.write("**Recommendation:**", recommendation)

        if latest:
            st.write(
                "**Soil condition:**",
                latest.get("soil_condition", "UNKNOWN")
            )

    with right:
        st.markdown("### Device Status")

        st.write(
            "**Device:**",
            "ONLINE" if device_online else "OFFLINE"
        )

        st.write(
            "**MQTT broker:**",
            "CONNECTED" if broker_connected else "DISCONNECTED"
        )

        if last_received:
            st.write(
                "**Last message:**",
                last_received.strftime("%Y-%m-%d %H:%M:%S UTC")
            )

    if not latest:
        st.warning("Waiting for the ESP8266 to publish sensor readings.")

# =========================================================
# PAGE: LIVE MONITORING
# =========================================================

elif page == "Live Sensor Monitoring":

    st.subheader("Live Sensor Monitoring")

    if latest is None:
        st.warning("No sensor data has been received yet.")

    else:
        c1, c2, c3 = st.columns(3)

        c1.metric(
            "Temperature",
            f"{temperature:.1f} °C" if temperature is not None else "N/A"
        )

        c2.metric(
            "Humidity",
            f"{humidity:.1f} %" if humidity is not None else "N/A"
        )

        c3.metric(
            "Soil Moisture",
            f"{soil:.1f} %" if soil is not None else "N/A"
        )

        st.divider()

        st.write("**Raw soil reading:**", soil_raw)
        st.write(
            "**Soil condition:**",
            latest.get("soil_condition", "UNKNOWN")
        )

        st.write(
            "**Device environmental risk:**",
            risk
        )

        st.write(
            "**Buzzer mode:**",
            latest.get("buzzer_mode", "UNKNOWN")
        )

        st.write(
            "**Fan mode:**",
            latest.get("fan_mode", "UNKNOWN")
        )

        st.write(
            "**Relay state:**",
            latest.get("relay_state", "UNKNOWN")
        )

# =========================================================
# PAGE: ENVIRONMENTAL RISK
# =========================================================

elif page == "Environmental Risk":

    st.subheader("Environmental Risk Assessment")

    if latest is None:
        st.warning("Waiting for sensor readings.")

    else:
        if "HIGH" in str(risk).upper():
            st.error("HIGH RISK")

        elif "WARNING" in str(risk).upper():
            st.warning("WARNING")

        elif "NORMAL" in str(risk).upper():
            st.success("NORMAL")

        else:
            st.info("Risk not available.")

        c1, c2, c3 = st.columns(3)

        c1.metric("Temperature", f"{temperature:.1f} °C")
        c2.metric("Humidity", f"{humidity:.1f} %")
        c3.metric("Soil moisture", f"{soil:.1f} %")

        st.info(recommendation)

        st.markdown("### Thresholds")

        st.write("- Temperature warning: 35 °C")
        st.write("- Temperature high: 40 °C")
        st.write("- Humidity warning: 70 %")
        st.write("- Humidity high: 80 %")

        st.caption(
            "These are the example thresholds in the firmware. "
            "Validate them for the crop and storage conditions."
        )

# =========================================================
# PAGE: HISTORICAL ANALYTICS
# =========================================================

elif page == "Historical Analytics":

    st.subheader("Historical Analytics")

    if history_df.empty:
        st.warning("No readings received during this dashboard session.")

    else:
        st.write(f"Records received: {len(history_df)}")

        columns = [
            c for c in [
                "temperature",
                "humidity",
                "soil_moisture_percent"
            ]
            if c in history_df.columns
        ]

        selected = st.multiselect(
            "Select parameters",
            columns,
            default=columns
        )

        if selected and "Time" in history_df.columns:

            long_df = history_df[
                ["Time"] + selected
            ].melt(
                id_vars="Time",
                var_name="Parameter",
                value_name="Reading"
            )

            fig = px.line(
                long_df,
                x="Time",
                y="Reading",
                color="Parameter",
                markers=True,
                title="Sensor History"
            )

            fig.update_layout(template="plotly_dark")

            st.plotly_chart(
                fig,
                use_container_width=True
            )

        st.dataframe(
            history_df,
            use_container_width=True
        )

        csv = history_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            "Download CSV",
            csv,
            "post_harvest_history.csv",
            "text/csv"
        )

        st.caption(
            "History is kept in dashboard memory and may be lost "
            "when the app restarts or its session is reset."
        )

# =========================================================
# PAGE: ALERTS
# =========================================================

elif page == "Alerts & Recommendations":

    st.subheader("Alerts & Recommendations")

    if latest is None:
        st.warning("No current sensor data.")

    else:
        if "HIGH" in str(risk).upper():
            st.error(
                "HIGH RISK: Inspect stored produce and take corrective action."
            )

        elif "WARNING" in str(risk).upper():
            st.warning(
                "WARNING: Inspect storage conditions and ventilation."
            )

        elif "NORMAL" in str(risk).upper():
            st.success(
                "NORMAL: Continue monitoring storage conditions."
            )

        st.info(recommendation)

        st.markdown("### Suggested actions")

        if "HIGH" in str(risk).upper():
            st.write("- Inspect stored produce.")
            st.write("- Check storage ventilation.")
            st.write("- Investigate possible spoilage.")
            st.write("- Check sensor readings and equipment.")

        elif "WARNING" in str(risk).upper():
            st.write("- Inspect storage conditions.")
            st.write("- Monitor temperature and humidity.")
            st.write("- Check ventilation.")

        else:
            st.write("- Continue normal monitoring.")
            st.write("- Maintain suitable storage conditions.")

# =========================================================
# PAGE: REMOTE DEVICE CONTROL
# =========================================================

elif page == "Remote Device Control":

    st.subheader("Remote Hardware Control")

    st.write(
        "Commands are published to the MQTT broker. "
        "The ESP8266 must be online to receive them."
    )

    st.warning(
        "Test relays without a connected load first. "
        "Use appropriate electrical isolation and protection."
    )

    st.markdown("### Buzzer")

    buzzer = st.radio(
        "Buzzer mode",
        ["AUTO", "ON", "OFF"],
        horizontal=True,
        key="buzzer_control"
    )

    st.markdown("### Fan")

    fan = st.radio(
        "Fan mode",
        ["AUTO", "ON", "OFF"],
        horizontal=True,
        key="fan_control"
    )

    st.markdown("### Additional relay")

    relay = st.radio(
        "Relay state",
        ["OFF", "ON"],
        horizontal=True,
        key="relay_control"
    )

    if st.button(
        "SEND COMMAND TO ESP8266",
        type="primary",
        use_container_width=True
    ):

        try:
            command = send_command(
                buzzer=buzzer,
                fan=fan,
                relay=relay
            )

            st.success(
                "Command published to MQTT broker."
            )

            st.json(command)

            st.caption(
                "Publishing confirms broker acceptance, "
                "not physical execution. Check the device's "
                "reported states below."
            )

        except Exception as error:
            st.error(str(error))

    st.divider()

    st.markdown("### Reported hardware states")

    if latest:
        st.write(
            "**Buzzer:**",
            latest.get("buzzer_state", "UNKNOWN")
        )

        st.write(
            "**Fan:**",
            latest.get("fan_state", "UNKNOWN")
        )

        st.write(
            "**Relay:**",
            latest.get("relay_state", "UNKNOWN")
        )

    else:
        st.info("No hardware status received yet.")

# =========================================================
# PAGE: SYSTEM INFORMATION
# =========================================================

elif page == "System Information":

    st.subheader("System Information")

    st.markdown("""
    ### Project
    Machine Learning-Based System for Predicting and
    Reducing Post-Harvest Losses Among Farmers.

    ### Hardware
    - ESP8266 NodeMCU ESP-12E
    - DHT11 temperature and humidity sensor
    - YL-69 soil moisture sensor
    - Buzzer
    - Push button
    - Fan relay module
    - Additional relay module

    ### Software
    - Arduino IDE
    - Python
    - Streamlit
    - MQTT
    - EMQX public MQTT broker
    - Pandas
    - Plotly

    ### Communication
    ESP8266 publishes sensor data to the MQTT telemetry topic.
    The Streamlit dashboard subscribes to the topic.
    The dashboard publishes hardware commands.
    The ESP8266 subscribes to the command topic.

    ### Machine Learning
    This version uses threshold-based environmental risk
    assessment. It does not claim to have a validated
    machine-learning model.
    """)

    st.markdown("### MQTT topics")

    st.code(
        f"Telemetry: {TELEMETRY_TOPIC}\n"
        f"Commands: {COMMAND_TOPIC}\n"
        f"Status: {STATUS_TOPIC}"
    )

# =========================================================
# FOOTER
# =========================================================

st.divider()

st.caption(
    "Post-Harvest Monitoring System | ESP8266 + MQTT + Streamlit"
)

# =========================================================
# AUTOMATIC REFRESH
# =========================================================

@st.fragment(run_every=3)
def live_connection_status():

    latest_now, _, last_now, broker_now, error_now = get_data()

    online_now = determine_connection(last_now)

    col1, col2 = st.columns(2)

    with col1:
        st.caption(
            "MQTT: CONNECTED" if broker_now else "MQTT: DISCONNECTED"
        )

    with col2:
        st.caption(
            "DEVICE: ONLINE" if online_now else "DEVICE: OFFLINE"
        )


live_connection_status()
