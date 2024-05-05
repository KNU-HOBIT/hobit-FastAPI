import paho.mqtt.client as mqtt
import threading

broker_address = "192.168.0.141"

# subscriber callback
def on_message_callback(client, userdata, message):
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic= ", message.topic)
    print("message qos=", message.qos)
    print("message retain flag= ", message.retain)


def test(topic):
    client1 = mqtt.Client(client_id=topic)

    client1.connect(host=broker_address) # 현재 문제가 되는 오류 부분

    client1.subscribe(topic=topic) 
    client1.on_message = on_message_callback
    
    client1.loop_forever()

def ThreadTest(topic):
    thread = threading.Thread(target=test, args=(topic,))
    thread.start()
