import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

#  Cargar el dataset limpio
df = pd.read_csv('dataset_original.csv', sep=',')

#  Repetir los pasos de limpieza
df['Defecto'] = (df['Piezas_Defectuosas'] > 0).astype(int)
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y', errors='coerce')
df['Año'] = df['Fecha'].dt.year
df['Mes'] = df['Fecha'].dt.month
df['Día'] = df['Fecha'].dt.day
df['Día_Semana'] = df['Fecha'].dt.dayofweek

#  Convertir variables categóricas a números
label_cols = ['Linea', 'MachineID', 'Operador', 'Turno']
encoders = {}
for col in label_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    encoders[col] = le  # por si luego quieres revertirlo

#  Definir variables X (entrada) e y (salida)
X = df[['Linea', 'MachineID', 'Operador', 'Turno', 'Año', 'Mes', 'Día', 'Día_Semana']]
y = df['Defecto']

#  Dividir en entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

#  Entrenar modelo (Random Forest)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

#  Evaluar el modelo
y_pred = model.predict(X_test)
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

print(" Métricas del modelo:")
print("Accuracy:", accuracy_score(y_test, y_pred))
print("Precision:", precision_score(y_test, y_pred))
print("Recall:", recall_score(y_test, y_pred))
print("F1-score:", f1_score(y_test, y_pred))

# Guardar el modelo entrenado
import joblib
joblib.dump(model, 'modelo_produccion.pkl')
print("Modelo guardado como 'modelo_produccion.pkl'")

