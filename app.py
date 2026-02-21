from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import tempfile
from datetime import datetime

app = Flask(__name__, static_folder='static')
CORS(app)

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
DRIVE_FOLDER_ID = "1o3fD5O3N65DQjXlzjmo_NfMAgCzTvIDv"
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT", "{}"))

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# ── GOOGLE DRIVE ───────────────────────────────────────────────────────────────
def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO, scopes=SCOPES
)
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
            return None, None, "No se encontraron los archivos en Drive. Asegúrate de que 'pedidos.xlsx' y 'cargas.xlsx' están en la carpeta."

        df_pedidos = pd.read_excel(pedidos_buf)
        df_cargas = pd.read_excel(cargas_buf)

        # Normalizar columna de pedido
        for df in [df_pedidos, df_cargas]:
            for col in df.columns:
                if "pedido" in col.lower() or col.lower() in ["nº pedido", "n pedido", "pedido"]:
                    df.rename(columns={col: "Pedido"}, inplace=True)
                    df["Pedido"] = df["Pedido"].astype(str).str.strip()
                    break

        return df_pedidos, df_cargas, None
    except Exception as e:
        return None, None, str(e)

def dataframes_to_context(df_pedidos, df_cargas):
    ctx = f"=== CUADRE DE PEDIDOS ===\n{df_pedidos.to_string(index=False)}\n\n"
    ctx += f"=== HOJA DE CARGAS (TRANSPORTE) ===\n{df_cargas.to_string(index=False)}\n"
    return ctx

# ── RUTAS ──────────────────────────────────────────────────────────────────────
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
        return jsonify({"error": "Pregunta vacía"}), 400

    df_pedidos, df_cargas, error = load_data()
    if error:
        return jsonify({"respuesta": f"⚠️ No puedo cargar los datos: {error}"})

    contexto = dataframes_to_context(df_pedidos, df_cargas)

    system_prompt = f"""Eres el asistente de almacén de ICA PRODUKT. Tienes acceso a los datos del día de hoy: el cuadre de pedidos y la hoja de cargas/transporte.

Responde siempre en español, de forma clara y directa, como si hablaras con el jefe de almacén. 
Cuando te pregunten por un pedido concreto, muestra toda la información disponible de ambas hojas: producto, bultos, palets, transportista, matrícula, observaciones, etc.
Si hay incidencias o anomalías en un pedido (anulaciones, "no encontramos", etc.), indícalas claramente.
Si no encuentras un pedido, dilo claramente.

DATOS DE HOY:
{contexto}
"""

    # Construir historial para Gemini
    messages = []
    for h in historial[-10:]:  # Últimos 10 mensajes
        messages.append({"role": h["role"], "parts": [h["content"]]})
    messages.append({"role": "user", "parts": [pregunta]})

    try:
        chat_session = model.start_chat(history=messages[:-1])
        response = chat_session.send_message(
            f"{system_prompt}\n\nPregunta del jefe de almacén: {pregunta}"
        )
        return jsonify({"respuesta": response.text})
    except Exception as e:
        return jsonify({"respuesta": f"Error al conectar con la IA: {str(e)}"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
