from fastapi_mqtt.fastmqtt import FastMQTT
from fastapi import FastAPI, status
from fastapi_mqtt.config import MQTTConfig
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

mqtt_config = MQTTConfig(
    host="127.0.0.1",
    port=1883,
    keepalive=30
)

mqtt = FastMQTT(config=mqtt_config)

mqtt.init_app(app)

class Registro(BaseModel):
    id: Optional[int] = None
    pressao: float
    vazao: float
    nivel: float
    consumo: float

registros = {
    1:{
        "pressao":2.0,
        "vazao":2.0,
        "nivel":2.0,
        "consumo":2.9
    },

    2:{
        "pressao":2.0,
        "vazao":3.0,
        "nivel":2.3,
        "consumo":2.1
    }
}


@mqtt.on_connect()
def connect(client, flags, rc, properties):
    mqtt.client.subscribe("/hidrometricos") #subscribing mqtt topic
    print("Connected: ", client, flags, rc, properties)

@mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ",topic, payload.decode(), qos, properties)
    return 0

@mqtt.subscribe("/broker/acqualog/hidrometricos")
async def message_to_topic(client, topic, payload, qos, properties):
    print("Received message to specific topic: ", topic, payload.decode(), qos, properties)

@mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")

@mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)


@app.get("/")
async def func():
    mqtt.publish("/hidrometricos", "Pronto para para receber os níveis hidrometricos") #publishing mqtt topic

    return {"resultado": True,"msg":"mensagem publicada" }

@app.post("/", status_code=status.HTTP_201_CREATED)
async def post_hidro(regi: Registro):
    del regi.id
    mqtt.publish("/hidrometricos", str(regi))
    next_id: int = len(registros) + 1
    registros[next_id] = regi
    return regi

if __name__ == '__main__':
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info", reload=True)
