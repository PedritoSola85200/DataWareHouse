import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
import joblib

#  Cargar el dataset limpio
df = pd.read_csv('dataset_original.csv', sep=',')

#  Limpieza y creación de variables
df['Defecto'] = (df['Piezas_Defectuosas'] > 0).astype(int)
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y', errors='coerce')
df['Año'] = df['Fecha'].dt.year
df['Mes'] = df['Fecha'].dt.month
df['Día'] = df['Fecha'].dt.day
df['Día_Semana'] = df['Fecha'].dt.dayofweek

#  Codificar variables categóricas
label_cols = ['Linea', 'MachineID', 'Operador', 'Turno']
encoders = {}
for col in label_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    encoders[col] = le

#  Variables de entrada (X) y salida (y)
X = df[['Linea', 'MachineID', 'Operador', 'Turno', 'Año', 'Mes', 'Día', 'Día_Semana']]
y = df['Defecto']

#  División en entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

#  Modelo base
modelo_base = RandomForestClassifier(n_estimators=100, random_state=42)
modelo_base.fit(X_train, y_train)

#  Evaluación inicial
y_pred_base = modelo_base.predict(X_test)
print("📊 Evaluación del modelo base:")
print("Accuracy:", accuracy_score(y_test, y_pred_base))
print("Precision:", precision_score(y_test, y_pred_base))
print("Recall:", recall_score(y_test, y_pred_base))
print("F1-score:", f1_score(y_test, y_pred_base))

#  Optimización del modelo con GridSearchCV
parametros = {
    'n_estimators': [50, 100, 200],
    'max_depth': [None, 10, 20],
    'min_samples_split': [2, 5, 10]
}

grid_search = GridSearchCV(modelo_base, parametros, cv=5, scoring='accuracy', n_jobs=-1)
grid_search.fit(X_train, y_train)

print("\n Mejor combinación de hiperparámetros encontrada:")
print(grid_search.best_params_)
print(" Mejor Accuracy obtenido:", grid_search.best_score_)

#  Reentrenar con los mejores parámetros
mejor_modelo = grid_search.best_estimator_
y_pred_opt = mejor_modelo.predict(X_test)

print("\nEvaluación del modelo optimizado:")
print("Accuracy:", accuracy_score(y_test, y_pred_opt))
print("Precision:", precision_score(y_test, y_pred_opt))
print("Recall:", recall_score(y_test, y_pred_opt))
print("F1-score:", f1_score(y_test, y_pred_opt))
print("\nReporte completo:\n", classification_report(y_test, y_pred_opt))

#  Guardar el mejor modelo
joblib.dump(mejor_modelo, 'modelo_produccion_optimo.pkl')
print("\n Modelo optimizado guardado como 'modelo_produccion_optimo.pkl'")
