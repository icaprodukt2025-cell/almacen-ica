from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import pandas as pd
import anthropic
from google.oauth2 import service_account
from googleapiclient.discovery import build
import io
from datetime import datetime

app = Flask(__name__, static_folder='static')
CORS(app)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SHEET_ID = "1S6uocKdf9o-IxWReBxq-1RoNtU_-AXtXe4qYPwv61-c"
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT", "{}"))
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)

def sheet_to_dataframe(service, sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=sheet_name
    ).execute()
    values = result.get("values", [])
    if not values or len(values) < 2:
        return pd.DataFrame()
    headers = values[0]
    rows = values[1:]
    # Igualar longitud de filas con cabecera
    rows_norm = [r + [""] * (len(headers) - len(r)) for r in rows]
    return pd.DataFrame(rows_norm, columns=headers)

def load_data():
    try:
        service = get_sheets_service()
        df_pedidos = sheet_to_dataframe(service, "Pedidos")
        df_cargas = sheet_to_dataframe(service, "Cargas")

        if df_pedidos.empty and df_cargas.empty:
            return None, None, "Las hojas estan vacias. Ejecuta los scripts de Gmail primero."

        # Normalizar columna Pedido
        for df in [df_pedidos, df_cargas]:
            for col in df.columns:
                if "pedido" in col.lower():
                    df.rename(columns={col: "Pedido"}, inplace=True)
                    df["Pedido"] = df["Pedido"].astype(str).str.strip()
                    break

        return df_pedidos, df_cargas, None
    except Exception as e:
        return None, None, str(e)

def dataframes_to_context(df_pedidos, df_cargas):
    # Filtrar cargas de hoy si tiene columna de fecha
    hoy = datetime.now().strftime("%d/%m/%Y")
    if not df_cargas.empty:
        fecha_col = df_cargas.columns[0]
        df_cargas_hoy = df_cargas[df_cargas[fecha_col].astype(str).str.startswith(hoy)]
        if df_cargas_hoy.empty:
            df_cargas_hoy = df_cargas.tail(60)
    else:
        df_cargas_hoy = df_cargas

    # Limitar pedidos recientes (ultimas 48h aprox = 200 filas)
    df_p = df_pedidos.tail(200)

    ctx = "=== PEDIDOS ===\n"
    ctx += df_p.to_string(index=False) if not df_p.empty else "Sin datos"
    ctx += "\n\n=== CARGAS Y MATRICULAS ===\n"
    ctx += df_cargas_hoy.to_string(index=False) if not df_cargas_hoy.empty else "Sin datos"
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
    hoy = datetime.now().strftime("%d/%m/%Y")
    cargas_hoy = 0
    if not df_cargas.empty:
        fecha_col = df_cargas.columns[0]
        cargas_hoy = len(df_cargas[df_cargas[fecha_col].astype(str).str.startswith(hoy)])
    return jsonify({
        "ok": True,
        "pedidos": int(pedidos_count),
        "lineas_carga": int(cargas_hoy) if cargas_hoy else len(df_cargas),
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

    system_prompt = f"""Eres el asistente de almacen de ICA PRODUKT. Tienes acceso a los pedidos y cargas del dia.
Responde en espanol, de forma clara y directa, como si hablaras con el jefe de almacen.
Para pedidos concretos muestra toda la info: producto, bultos, palets, transportista, matricula, observaciones.
Si hay campos vacios que deberian tener valor (matricula, transportista) indicalos como pendientes.
Si no encuentras un pedido dilo claramente.

DATOS ACTUALES:
{contexto}
"""

    messages = []
    for h in historial[-6:]:
        role = "user" if h["role"] == "user" else "assistant"
        messages.append({"role": role, "content": h["content"]})
    messages.append({"role": "user", "content": pregunta})

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            system=system_prompt,
            messages=messages
        )
        return jsonify({"respuesta": response.content[0].text})
    except anthropic.RateLimitError:
        return jsonify({"respuesta": "Demasiadas peticiones. Espera unos segundos e intentalo de nuevo."})
    except Exception as e:
        return jsonify({"respuesta": f"Error: {str(e)}"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
