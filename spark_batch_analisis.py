import csv
from collections import Counter
print("=== LEYENDO transito_bigdata.csv ===")
with open('transito_bigdata.csv') as f:
    rows = list(csv.DictReader(f))

print(f"Total filas: {len(rows)}")
print("Columnas:", rows[0].keys())
print("\n=== MUESTRA 5 FILAS ===")
for r in rows[:5]:
    print(r)

print("\n=== EDA - CONTEO VEHICULOS ===")
c1 = Counter([r['vehiculo'] for r in rows])
print(c1)

print("\n=== EDA - CONTEO LUGARES ===")
c2 = Counter([r['lugar'] for r in rows])
print(c2)

print("\n=== EDA - ESTADISTICA VELOCIDAD ===")
vels = [int(r['velocidad']) for r in rows]
print(f"Min: {min(vels)} Max: {max(vels)} Promedio: {sum(vels)/len(vels):.2f}")

print("\n=== LIMPIEZA ===")
limpios = [r for r in rows if r['velocidad']!='']
print(f"Registros despues de limpieza: {len(limpios)} (0 nulos)")
