from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import pandas as pd
import anthropic
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from datetime import datetime

app = Flask(__name__, static_folder='static')
CORS(app)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DRIVE_FOLDER_ID = "1o3fD5O3N65DQjXlzjmo_NfMAgCzTvIDv"
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT", "{}"))
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def download_excel_from_drive(service, filename):
    results = service.files().list(
        q=f"name='{filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name, modifiedTime)"
    ).execute()
    files = results.get("files", [])
    if not files:
        return None
    file_id = files[0]["id"]
    request_dl = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request_dl)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf

def load_data():
    try:
        service = get_drive_service()
        pedidos_buf = download_excel_from_drive(service, "pedidos.xlsx")
        cargas_buf = download_excel_from_drive(service, "cargas.xlsx")

        if pedidos_buf is None or cargas_buf is None:
            return None, None, "No se encontraron los archivos en Drive."

        df_pedidos = pd.read_excel(pedidos_buf, engine="openpyxl")

        df_cargas_raw = pd.read_excel(cargas_buf, engine="openpyxl", usecols=range(12))
        df_cargas_raw = df_cargas_raw.dropna(how='all', axis=1)

        fecha_col = df_cargas_raw.columns[0]
        hoy = pd.Timestamp(datetime.now().date())
        df_cargas_raw[fecha_col] = pd.to_datetime(df_cargas_raw[fecha_col], errors='coerce')
        df_cargas = df_cargas_raw[df_cargas_raw[fecha_col] == hoy].copy()

        if df_cargas.empty:
            df_cargas = df_cargas_raw[df_cargas_raw[fecha_col].notna()].copy()

        for df in [df_pedidos, df_cargas]:
            for col in df.columns:
                if isinstance(col, str) and ("pedido" in col.lower()):
                    df.rename(columns={col: "Pedido"}, inplace=True)
                    df["Pedido"] = df["Pedido"].astype(str).str.strip()
                    break

        return df_pedidos, df_cargas, None
    except Exception as e:
        return None, None, str(e)

def dataframes_to_context(df_pedidos, df_cargas):
    ctx = "=== CUADRE DE PEDIDOS ===\n"
    ctx += df_pedidos.to_string(index=False)
    ctx += "\n\n=== HOJA DE CARGAS (TRANSPORTE) ===\n"
    ctx += df_cargas.to_string(index=False)
    return ctx

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/status")
def status():
    df_pedidos, df_cargas, error = load_data()
    if error:
        return jsonify({"ok": False, "error": error})
    pedidos_count = df_pedidos["Pedido"].nunique() if "Pedido" in df_pedidos.columns else len(df_pedidos)
    return jsonify({
        "ok": True,
        "pedidos": int(pedidos_count),
        "lineas_carga": len(df_cargas),
        "fecha": datetime.now().strftime("%d/%m/%Y %H:%M")
    })

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.json
    pregunta = data.get("pregunta", "").strip()
    historial = data.get("historial", [])

    if not pregunta:
        return jsonify({"error": "Pregunta vacia"}), 400

    df_pedidos, df_cargas, error = load_data()
    if error:
        return jsonify({"respuesta": f"No puedo cargar los datos: {error}"})

    contexto = dataframes_to_context(df_pedidos, df_cargas)

    system_prompt = f"""Eres el asistente de almacen de ICA PRODUKT. Tienes acceso a los datos del dia de hoy.
Responde siempre en espanol, de forma clara y directa.
Cuando te pregunten por un pedido concreto, muestra toda la informacion disponible.
Si hay incidencias o anomalias, indicalas claramente.
Si no encuentras un pedido, dilo claramente.

DATOS DE HOY:
{contexto}
"""

    messages = []
    for h in historial[-10:]:
        role = "user" if h["role"] == "user" else "assistant"
        messages.append({"role": role, "content": h["content"]})
    messages.append({"role": "user", "content": pregunta})

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=system_prompt,
            messages=messages
        )
        return jsonify({"respuesta": response.content[0].text})
    except Exception as e:
        return jsonify({"respuesta": f"Error al conectar con la IA: {str(e)}"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)    df_cargas_raw[fecha_col] = pd.to_datetime(df_cargas_raw[fecha_col], errors='coerce')
    df_cargas = df_cargas_raw[df_cargas_raw[fecha_col] == hoy].copy()

    if df_cargas.empty:
        df_cargas = df_cargas_raw[df_cargas_raw[fecha_col].notna()].copy()

    for df in [df_pedidos, df_cargas]:
        for col in df.columns:
            if isinstance(col, str) and ("pedido" in col.lower()):
                df.rename(columns={col: "Pedido"}, inplace=True)
                df["Pedido"] = df["Pedido"].astype(str).str.strip()
                break

    return df_pedidos, df_cargas, None
except Exception as e:
    return None, None, str(e)
```

def dataframes_to_context(df_pedidos, df_cargas):
ctx = “=== CUADRE DE PEDIDOS ===\n”
ctx += df_pedidos.to_string(index=False)
ctx += “\n\n=== HOJA DE CARGAS (TRANSPORTE) ===\n”
ctx += df_cargas.to_string(index=False)
return ctx

@app.route(”/”)
def index():
return send_from_directory(“static”, “index.html”)

@app.route(”/api/status”)
def status():
df_pedidos, df_cargas, error = load_data()
if error:
return jsonify({“ok”: False, “error”: error})
pedidos_count = df_pedidos[“Pedido”].nunique() if “Pedido” in df_pedidos.columns else len(df_pedidos)
return jsonify({
“ok”: True,
“pedidos”: int(pedidos_count),
“lineas_carga”: len(df_cargas),
“fecha”: datetime.now().strftime(”%d/%m/%Y %H:%M”)
})

@app.route(”/api/chat”, methods=[“POST”])
def chat():
data = request.json
pregunta = data.get(“pregunta”, “”).strip()
historial = data.get(“historial”, [])

```
if not pregunta:
    return jsonify({"error": "Pregunta vacia"}), 400

df_pedidos, df_cargas, error = load_data()
if error:
    return jsonify({"respuesta": f"No puedo cargar los datos: {error}"})

contexto = dataframes_to_context(df_pedidos, df_cargas)

system_prompt = f"""Eres el asistente de almacen de ICA PRODUKT. Tienes acceso a los datos del dia de hoy.
```

Responde siempre en espanol, de forma clara y directa.
Cuando te pregunten por un pedido concreto, muestra toda la informacion disponible.
Si hay incidencias o anomalias, indicalas claramente.
Si no encuentras un pedido, dilo claramente.

DATOS DE HOY:
{contexto}
“””

```
messages = []
for h in historial[-10:]:
    role = "user" if h["role"] == "user" else "assistant"
    messages.append({"role": role, "content": h["content"]})
messages.append({"role": "user", "content": pregunta})

try:
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=system_prompt,
        messages=messages
    )
    return jsonify({"respuesta": response.content[0].text})
except Exception as e:
    return jsonify({"respuesta": f"Error al conectar con la IA: {str(e)}"})
```

if **name** == “**main**”:
port = int(os.environ.get(“PORT”, 5000))
app.run(host=“0.0.0.0”, port=port, debug=False)
