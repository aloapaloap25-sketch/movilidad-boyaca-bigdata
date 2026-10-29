from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from collections import defaultdict

consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Leyendo 30 datos para la gráfica...")
datos = defaultdict(list)

for i, msg in enumerate(consumer):
    data = msg.value
    print(f"Recibido: {data}")
    datos[data['sensor_id']].append(data['temperature'])
    if i >= 29:
        break

# Hacer gráfica
plt.figure()
for sensor, temps in datos.items():
    plt.plot(temps, label=f"Sensor {sensor}")
    
plt.title("Temperatura por Sensor - Kafka")
plt.xlabel("Lectura")
plt.ylabel("Temperatura °C")
plt.legend()
plt.savefig("figura_final.png")
print("¡Figura guardada como figura_final.png!")
plt.show()
