from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import pandas as pd
import anthropic
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime

app = Flask(__name__, static_folder='static')
CORS(app)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SHEET_ID = "1S6uocKdf9o-IxWReBxq-1RoNtU_-AXtXe4qYPwv61-c"
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT", "{}"))
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
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
    # Normalizar longitud de filas
    rows_norm = []
    for r in rows:
        if len(r) < len(headers):
            r = r + [""] * (len(headers) - len(r))
        else:
            r = r[:len(headers)]
        rows_norm.append(r)
    return pd.DataFrame(rows_norm, columns=headers)

def load_data():
    try:
        service = get_sheets_service()
        df_pedidos = sheet_to_dataframe(service, "Pedidos")
        df_cargas = sheet_to_dataframe(service, "Cargas")

        if df_pedidos.empty and df_cargas.empty:
            return None, None, "Las hojas estan vacias."

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
    hoy = datetime.now().strftime("%d/%m/%Y")

    # Cargas de hoy
    if not df_cargas.empty:
        fecha_col = df_cargas.columns[0]
        df_cargas_hoy = df_cargas[df_cargas[fecha_col].astype(str).str.startswith(hoy)]
        if df_cargas_hoy.empty:
            df_cargas_hoy = df_cargas.tail(60)
    else:
        df_cargas_hoy = df_cargas

    # Pedidos recientes
    df_p = df_pedidos

    ctx = "=== PEDIDOS ===\n"
    ctx += df_p.to_string(index=False) if not df_p.empty else "Sin datos"
    ctx += "\n\n=== CARGAS Y MATRICULAS ===\n"
    ctx += df_cargas_hoy.to_string(index=False) if not df_cargas_hoy.empty else "Sin datos"
    return ctx

@app.route("/")
def index():
    response = send_from_directory("static", "index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/api/status")
def status():
    df_pedidos, df_cargas, error = load_data()
    if error:
        return jsonify({"ok": False, "error": error})
    hoy = datetime.now().strftime("%d/%m/%Y")
    # Pedidos de hoy
    pedidos_hoy = 0
    if not df_pedidos.empty:
        for col in df_pedidos.columns:
            col_str = str(col).lower()
            if "fecha" in col_str or "salida" in col_str or "entrega" in col_str:
                pedidos_hoy = len(df_pedidos[df_pedidos[col].apply(lambda x: normalizar_fecha(str(x)) == hoy)])
                break
        if pedidos_hoy == 0:
            # Try first date-like column
            for col in df_pedidos.columns:
                sample = df_pedidos[col].dropna().head(5).apply(lambda x: normalizar_fecha(str(x)))
                if sample.str.match(r"\d{2}/\d{2}/\d{4}").any():
                    pedidos_hoy = len(df_pedidos[df_pedidos[col].apply(lambda x: normalizar_fecha(str(x)) == hoy)])
                    break
    cargas_hoy = 0
    if not df_cargas.empty:
        fecha_col = df_cargas.columns[0]
        cargas_hoy = len(df_cargas[df_cargas[fecha_col].astype(str).apply(lambda x: normalizar_fecha(x)).str.startswith(hoy[:10])])
    return jsonify({
        "ok": True,
        "pedidos_hoy": int(pedidos_hoy),
        "lineas_carga": int(cargas_hoy),
        "fecha": hoy
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

    system_prompt = f"""Eres el asistente de almacen de ICA PRODUKT. Tienes acceso a los pedidos y cargas.
Responde en espanol, de forma clara y directa, como si hablaras con el jefe de almacen.
Para pedidos concretos muestra toda la info: producto, bultos, palets, transportista, matricula, observaciones.
Si hay campos vacios que deberian tener valor (matricula, transportista) indicalos como pendientes.
Si no encuentras un pedido dilo claramente.

DATOS:
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

@app.route("/api/planificacion")
def planificacion():
    df_pedidos, _, error = load_data()
    if error:
        return jsonify({"error": error})
    if df_pedidos.empty:
        return jsonify({"dias": {}})

    # Buscar columna fecha salida
    fecha_col = None
    for col in df_pedidos.columns:
        if "salida" in col.lower():
            fecha_col = col
            break
    if not fecha_col:
        # Mostrar columnas disponibles para debug
        return jsonify({"error": "No columna Fecha Salida. Columnas: " + str(list(df_pedidos.columns))})

    # DEBUG: ver muestra de fechas
    sample_fechas = df_pedidos[fecha_col].tail(10).astype(str).tolist()

    # Agrupar por fecha de salida
    def normalizar_fecha(f):
        from datetime import datetime, timedelta
        val = str(f).strip()
        if not val or val in ('', 'None', 'nan'):
            return ''
        # Si es numero serial de Excel (ej: 45677)
        if val.isdigit() and len(val) == 5:
            try:
                # Excel epoch: 1 enero 1900, pero con bug del anno bisiesto 1900
                excel_date = int(val)
                fecha = datetime(1899, 12, 30) + timedelta(days=excel_date)
                return fecha.strftime("%d/%m/%Y")
            except:
                pass
        # Si ya viene como dd/mm/yyyy o d/m/yyyy
        try:
            partes = val.split("/")
            if len(partes) == 3:
                d = partes[0].zfill(2)
                m = partes[1].zfill(2)
                y = partes[2]
                return f"{d}/{m}/{y}"
        except:
            pass
        return val

    dias = {}
    for _, row in df_pedidos.iterrows():
        fecha = normalizar_fecha(row.get(fecha_col, ""))
        if not fecha or fecha == "None" or fecha == "":
            continue
        if fecha not in dias:
            dias[fecha] = []
        dias[fecha].append({
            "pedido": str(row.get("Pedido", "")),
            "cliente": str(row.get("Cliente", "")),
            "producto": str(row.get("Producto", "")),
            "bultos": str(row.get("Bultos", "")),
            "palets": str(row.get("Palets", "")),
            "transportista": str(row.get("Transportista", "")),
            "matricula": str(row.get("Matricula", ""))
        })

    return jsonify({"dias": dias, "debug_fechas": sample_fechas})


@app.route("/api/alertas")
def alertas():
    df_pedidos, df_cargas, error = load_data()
    if error:
        return jsonify({"error": error, "alertas": []})

    if df_cargas.empty:
        return jsonify({"alertas": []})

    # Pedidos que hay en Cargas
    pedidos_col_cargas = None
    for col in df_cargas.columns:
        if "pedido" in col.lower():
            pedidos_col_cargas = col
            break

    if not pedidos_col_cargas:
        return jsonify({"alertas": []})

    # Pedidos que hay en la tabla Pedidos
    pedidos_conocidos = set()
    if not df_pedidos.empty and "Pedido" in df_pedidos.columns:
        pedidos_conocidos = set(df_pedidos["Pedido"].astype(str).str.strip().tolist())

    # Buscar pedidos de Cargas que no estan en Pedidos
    alertas_list = []
    hoy = datetime.now().strftime("%d/%m/%Y")

    for _, row in df_cargas.iterrows():
        fecha = str(row.get(df_cargas.columns[0], "")).strip()
        # Solo alertas de hoy o recientes
        if not fecha.startswith(hoy):
            continue
        num_pedido = str(row.get(pedidos_col_cargas, "")).strip()
        if not num_pedido or num_pedido == "nan" or num_pedido == "":
            continue
        if num_pedido not in pedidos_conocidos:
            alertas_list.append({
                "pedido": num_pedido,
                "destino": str(row.get("Destino/Referencia", "") or row.get("Destino", "") or ""),
                "mercancia": str(row.get("Mercancia", "") or row.get("Mercancia", "") or ""),
                "palets": str(row.get("Palets", "") or row.get("Palet", "") or ""),
                "transportista": str(row.get("Transportista", "") or ""),
                "matricula": str(row.get("Matricula", "") or row.get("Matricula", "") or "")
            })

    return jsonify({"alertas": alertas_list, "total": len(alertas_list)})


@app.route("/api/cargas")
def cargas():
    df_pedidos, df_cargas, error = load_data()
    if error:
        return jsonify({"error": error, "cargas": []})

    hoy = datetime.now().strftime("%d/%m/%Y")

    # Build matriculas/transportista lookup from Cargas sheet (by pedido number)
    cargas_info = {}
    if not df_cargas.empty:
        for _, row in df_cargas.iterrows():
            pedido = str(row.get("Pedido", "") or "").strip()
            if not pedido or pedido == "nan":
                continue
            matricula = str(row.get("Matricula", "") or "").strip()
            transportista = str(row.get("Transportista", "") or "").strip()
            obs = str(row.get("Observaciones", "") or "").strip()
            cargas_info[pedido] = {
                "matricula": "" if matricula == "nan" else matricula,
                "transportista": "" if transportista == "nan" else transportista,
                "observaciones": "" if obs == "nan" else obs,
            }

    # Use pedidos sheet as source of truth - find fecha_col
    fecha_col = None
    for col in df_pedidos.columns:
        if "salida" in col.lower():
            fecha_col = col
            break

    if not fecha_col or df_pedidos.empty:
        return jsonify({"cargas": [], "total": 0})

    items = []
    for _, row in df_pedidos.iterrows():
        fecha = normalizar_fecha(str(row.get(fecha_col, "")))
        if fecha != hoy:
            continue
        pedido = str(row.get("Pedido", "")).strip()
        if not pedido or pedido == "nan":
            continue
        extra = cargas_info.get(pedido, {})
        items.append({
            "pedido": pedido,
            "cliente": str(row.get("Cliente", "") or "").strip(),
            "producto": str(row.get("Producto", "") or "").strip(),
            "bultos": str(row.get("Bultos", "") or "").strip(),
            "palets": str(row.get("Palets", "") or "").strip(),
            "matricula": extra.get("matricula", ""),
            "transportista": extra.get("transportista", str(row.get("Transportista", "") or "").strip()),
            "observaciones": extra.get("observaciones", ""),
            "fecha": fecha
        })

    return jsonify({"cargas": items, "total": len(items)})

@app.route("/api/confeccion", methods=["POST"])
def guardar_confeccion():
    try:
        data = request.get_json()
        pedido = str(data.get("pedido", ""))
        producto = str(data.get("producto", ""))
        cliente = str(data.get("cliente", ""))
        palet = str(data.get("palet", ""))
        linea = str(data.get("linea", ""))
        personas = str(data.get("personas", ""))
        inicio = str(data.get("inicio", ""))
        fin = str(data.get("fin", ""))
        minutos = str(data.get("minutos", ""))
        lotes = data.get("lotes", [])
        fecha = datetime.now().strftime("%d/%m/%Y")

        service = get_sheets_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_names = [s["properties"]["title"] for s in spreadsheet["sheets"]]

        # Crear hoja Tiempos si no existe
        if "Tiempos" not in sheet_names:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": "Tiempos"}}}]}
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range="Tiempos!A1",
                valueInputOption="RAW",
                body={"values": [["Fecha", "Pedido", "Producto", "Cliente", "Palet",
                                  "Linea", "Personas", "Inicio", "Fin", "Minutos",
                                  "Lote", "Kg Usados", "Kg Destrio", "Bultos"]]}
            ).execute()

        # Crear hoja Confecciones si no existe (resumen sin tiempos)
        if "Confecciones" not in sheet_names:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": "Confecciones"}}}]}
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range="Confecciones!A1",
                valueInputOption="RAW",
                body={"values": [["Fecha", "Pedido", "Producto", "Cliente", "Palet",
                                  "Linea", "Personas", "Inicio", "Fin", "Minutos",
                                  "Lote", "Kg Usados", "Kg Destrio"]]}
            ).execute()

        rows_tiempos = []
        rows_confecciones = []

        # Get bultos from pedidos for this pedido number
        bultos_pedido = ""
        try:
            df_pedidos, _, _ = load_data()
            if not df_pedidos.empty and "Pedido" in df_pedidos.columns:
                match = df_pedidos[df_pedidos["Pedido"].astype(str) == pedido]
                if not match.empty:
                    bultos_pedido = str(match.iloc[0].get("Bultos", ""))
        except:
            pass

        for lote in lotes:
            row_base = [
                fecha, pedido, producto, cliente, palet,
                linea, personas, inicio, fin, minutos,
                lote.get("lote", ""),
                lote.get("kg_usados", 0),
                lote.get("kg_destrio", 0)
            ]
            rows_confecciones.append(row_base)
            rows_tiempos.append(row_base + [bultos_pedido])

        if rows_tiempos:
            service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID, range="Tiempos!A1",
                valueInputOption="RAW", insertDataOption="INSERT_ROWS",
                body={"values": rows_tiempos}
            ).execute()

        if rows_confecciones:
            service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID, range="Confecciones!A1",
                valueInputOption="RAW", insertDataOption="INSERT_ROWS",
                body={"values": rows_confecciones}
            ).execute()

        return jsonify({"ok": True, "filas": len(rows_tiempos)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
