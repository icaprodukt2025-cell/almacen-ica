from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import pandas as pd
import anthropic
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pdfplumber
import re
import io
import hashlib
import secrets

app = Flask(__name__, static_folder='static')
CORS(app)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SHEET_ID = "1S6uocKdf9o-IxWReBxq-1RoNtU_-AXtXe4qYPwv61-c"

# ===== USUARIOS =====
# Usuarios: { username: { password_hash, role, nombre } }
# Roles: admin, jefa, operario
USUARIOS = {
    "admin": {
        "hash": hashlib.sha256("ica2025admin".encode()).hexdigest(),
        "role": "admin",
        "nombre": "Administrador"
    },
    "almacen": {
        "hash": hashlib.sha256("ica2025".encode()).hexdigest(),
        "role": "jefa",
        "nombre": "Jefa Almacen"
    },
    "linea": {
        "hash": hashlib.sha256("linea2025".encode()).hexdigest(),
        "role": "operario",
        "nombre": "Operario Linea"
    }
}

# Sessions: token -> { username, role, nombre, expires }
SESSIONS = {}

def get_session(request):
    token = request.headers.get('X-Session-Token') or request.cookies.get('session_token')
    if not token:
        return None
    session = SESSIONS.get(token)
    if not session:
        return None
    if datetime.now() > session['expires']:
        del SESSIONS[token]
        return None
    return session

def require_auth(roles=None):
    def decorator(f):
        from functools import wraps
        @wraps(f)
        def wrapped(*args, **kwargs):
            session = get_session(request)
            if not session:
                return jsonify({"error": "No autorizado", "auth_required": True}), 401
            if roles and session['role'] not in roles:
                return jsonify({"error": "Sin permisos"}), 403
            return f(*args, **kwargs)
        return wrapped
    return decorator

@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    username = str(data.get("username", "")).strip().lower()
    password = str(data.get("password", ""))
    
    user = USUARIOS.get(username)
    if not user:
        return jsonify({"ok": False, "error": "Usuario o contraseña incorrectos"})
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if password_hash != user["hash"]:
        return jsonify({"ok": False, "error": "Usuario o contraseña incorrectos"})
    
    token = secrets.token_hex(32)
    SESSIONS[token] = {
        "username": username,
        "role": user["role"],
        "nombre": user["nombre"],
        "expires": datetime.now() + timedelta(hours=24)
    }
    
    return jsonify({
        "ok": True,
        "token": token,
        "role": user["role"],
        "nombre": user["nombre"]
    })

@app.route("/api/logout", methods=["POST"])
def logout():
    token = request.headers.get('X-Session-Token')
    if token and token in SESSIONS:
        del SESSIONS[token]
    return jsonify({"ok": True})

@app.route("/api/me")
def me():
    session = get_session(request)
    if not session:
        return jsonify({"auth_required": True}), 401
    return jsonify({"ok": True, "username": session["username"], "role": session["role"], "nombre": session["nombre"]})

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

def normalizar_fecha(f):
    val = str(f).strip()
    if not val or val in ('', 'None', 'nan'):
        return ''
    if val.isdigit() and len(val) == 5:
        try:
            excel_date = int(val)
            fecha = datetime(1899, 12, 30) + timedelta(days=excel_date)
            return fecha.strftime("%d/%m/%Y")
        except:
            pass
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


# ===== ESTADO COMPARTIDO =====
ESTADO_SHEET = "Estado"

def ensure_estado_sheet(service):
    spreadsheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    sheet_names = [s["properties"]["title"] for s in spreadsheet["sheets"]]
    if ESTADO_SHEET not in sheet_names:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": ESTADO_SHEET}}}]}
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{ESTADO_SHEET}!A1",
            valueInputOption="RAW",
            body={"values": [["Timestamp", "Pedido", "Tipo", "Valor"]]}
        ).execute()

@app.route("/api/estado", methods=["GET"])
def get_estado():
    """Return all estado entries from last 24h"""
    try:
        service = get_sheets_service()
        ensure_estado_sheet(service)
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{ESTADO_SHEET}!A:D"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return jsonify({"ok": True, "estados": [], "palets": []})

        cutoff = datetime.now() - timedelta(hours=24)
        estados = []
        palets = []

        for row in rows[1:]:
            if len(row) < 4:
                continue
            try:
                ts = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            except:
                continue
            if ts < cutoff:
                continue
            tipo = row[2]
            valor = row[3]
            if tipo == "estado_pedido":
                estados.append({"pedido": row[1], "estado": valor})
            elif tipo == "palet_activo":
                import json as json_mod
                try:
                    palets.append(json_mod.loads(valor))
                except:
                    pass
            elif tipo == "palet_finalizado":
                # Remove from palets if present
                palets = [p for p in palets if not (str(p.get("pedido")) == row[1] and str(p.get("key","")) == valor)]

        return jsonify({"ok": True, "estados": estados, "palets": palets})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "estados": [], "palets": []})

@app.route("/api/estado/debug")
def debug_estado():
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{ESTADO_SHEET}!A:D"
        ).execute()
        rows = result.get("values", [])
        return jsonify({"rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/estado", methods=["POST"])
def set_estado():
    """Save a state change"""
    try:
        import json as json_mod
        data = request.get_json(force=True)
        if not data:
            return jsonify({"ok": False, "error": "No JSON received"})
        service = get_sheets_service()
        ensure_estado_sheet(service)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        valor = data.get("valor", "")
        if isinstance(valor, dict):
            valor = json_mod.dumps(valor, ensure_ascii=False)
        elif not isinstance(valor, str):
            valor = str(valor)
        row = [[ts, str(data.get("pedido", "")), str(data.get("tipo", "")), valor]]
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{ESTADO_SHEET}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": row}
        ).execute()
        return jsonify({"ok": True})
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


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

# ===== IMPORTAR PROGRAMA PDF =====

def parse_programa_pdf(file_bytes):
    """Parse a programa PDF and return structured data"""
    import io
    resultado = {}
    pedidos = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        text = pdf.pages[0].extract_text()

    lines = text.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i]

        m = re.search(r'PROGRAMA SEMANAL N[\xba\.o]:\s*(\d+)\s*[-\u2013]\s*(\d+)', line)
        if m:
            resultado['programa'] = m.group(1)
            resultado['carga'] = m.group(2)

        m2 = re.search(r'REF\.\s*PROGRAMA:\s*(.+)', line)
        if m2:
            resultado['cliente'] = m2.group(1).strip()

        if 'PRESENTACION:' in line:
            # Product may be on same line or next
            after = line.split('PRESENTACION:')[-1].strip()
            if after:
                resultado['producto'] = after
            elif i + 1 < len(lines):
                resultado['producto'] = lines[i + 1].strip()

        m4 = re.search(r'BULTOS/PALET:\s*(\d+)', line)
        if m4:
            resultado['bultos_palet'] = int(m4.group(1))

        m5 = re.search(r'MARCA:\s*(.+)', line)
        if m5:
            resultado['marca'] = m5.group(1).strip()

        # Pedido row: 4 digits, carga, day name, date, palets, bultos, lote
        row = re.match(r'^(\d{4})\s+(\d+)\s+\w+\s+(\d{2}/\d{2}/\d{4})\s+([\d,\.]+)\s+(\d+)\s+(\d+)', line.strip())
        if row:
            pedidos.append({
                'pedido': row.group(1),
                'carga': row.group(2),
                'fecha_salida': row.group(3),
                'palets': float(row.group(4).replace(',', '.')),
                'bultos': int(row.group(5)),
                'lote': row.group(6),
            })

        i += 1

    resultado['pedidos'] = pedidos
    return resultado


@app.route("/api/importar-pdf", methods=["POST"])
def importar_programa():
    """Receive PDF, parse it, insert rows into Pedidos sheet"""
    try:
        if 'pdf' not in request.files:
            return jsonify({"ok": False, "error": "No se recibio PDF"})

        file = request.files['pdf']
        file_bytes = file.read()
        data = parse_programa_pdf(file_bytes)

        if not data.get('pedidos'):
            return jsonify({"ok": False, "error": "No se encontraron pedidos en el PDF", "debug": data})

        service = get_sheets_service()

        # Get current headers of Pedidos sheet to know column order
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range="Pedidos!1:1"
        ).execute()
        headers = result.get('values', [[]])[0]

        # Map headers to index
        def col(name):
            for i, h in enumerate(headers):
                if name.lower() in str(h).lower():
                    return i
            return -1

        producto = data.get('producto', '')
        cliente = data.get('cliente', '')
        marca = data.get('marca', '')
        programa = data.get('programa', '')
        bultos_palet = data.get('bultos_palet', '')

        rows_to_add = []
        for p in data['pedidos']:
            row = [''] * max(len(headers), 10)
            def set_col(name, val):
                idx = col(name)
                if idx >= 0:
                    row[idx] = val
            set_col('pedido', p['pedido'])
            set_col('fecha', p['fecha_salida'])
            set_col('salida', p['fecha_salida'])
            set_col('producto', producto)
            set_col('presentacion', producto)
            set_col('cliente', cliente if cliente else marca)
            set_col('bultos', p['bultos'])
            set_col('palet', p['palets'])
            set_col('carga', p['carga'])
            set_col('programa', programa)
            set_col('lote', p['lote'])
            rows_to_add.append(row[:len(headers)])

        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Pedidos!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_to_add}
        ).execute()

        return jsonify({
            "ok": True,
            "programa": programa,
            "cliente": cliente,
            "producto": producto,
            "pedidos_insertados": len(rows_to_add),
            "pedidos": data['pedidos']
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


# ===== INFORMES =====

@app.route("/api/informes/confeccion")
def informe_confeccion():
    """Coste real por confeccion usando datos de hoja Tiempos + precio hora"""
    try:
        service = get_sheets_service()
        
        # Read Tiempos sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range="Tiempos!A:N"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return jsonify({"ok": True, "registros": [], "resumen": {}})
        
        headers = rows[0]
        registros = []
        
        # Price per hour - can be overridden per request
        precio_hora = float(request.args.get("precio_hora", 10.5))
        
        # Date filter
        fecha_desde = request.args.get("desde", "")
        fecha_hasta = request.args.get("hasta", "")
        
        for row in rows[1:]:
            if len(row) < 10:
                continue
            try:
                fecha = row[0] if len(row) > 0 else ""
                pedido = row[1] if len(row) > 1 else ""
                producto = row[2] if len(row) > 2 else ""
                cliente = row[3] if len(row) > 3 else ""
                palet = row[4] if len(row) > 4 else ""
                linea = row[5] if len(row) > 5 else ""
                personas = int(row[6]) if len(row) > 6 and str(row[6]).isdigit() else 0
                inicio = row[7] if len(row) > 7 else ""
                fin = row[8] if len(row) > 8 else ""
                minutos = float(str(row[9]).replace(",",".")) if len(row) > 9 and row[9] else 0
                kg_usados = float(str(row[11]).replace(",",".")) if len(row) > 11 and row[11] else 0
                kg_destrio = float(str(row[12]).replace(",",".")) if len(row) > 12 and row[12] else 0
                bultos = row[13] if len(row) > 13 else ""

                # Date filter
                if fecha_desde and fecha < fecha_desde:
                    continue
                if fecha_hasta and fecha > fecha_hasta:
                    continue

                # Cost calculation
                horas = minutos / 60
                coste = round(horas * personas * precio_hora, 2)
                
                # Rendimiento: bultos por hora por persona
                try:
                    bul = int(str(bultos).replace(",","").split(".")[0]) if bultos else 0
                    rendimiento = round(bul / horas / personas, 1) if horas > 0 and personas > 0 else 0
                except:
                    rendimiento = 0

                registros.append({
                    "fecha": fecha,
                    "pedido": pedido,
                    "producto": producto,
                    "cliente": cliente,
                    "palet": palet,
                    "linea": linea,
                    "personas": personas,
                    "inicio": inicio,
                    "fin": fin,
                    "minutos": minutos,
                    "kg_usados": kg_usados,
                    "kg_destrio": kg_destrio,
                    "coste": coste,
                    "rendimiento": rendimiento
                })
            except Exception as row_err:
                continue
        
        # Resumen
        total_coste = round(sum(r["coste"] for r in registros), 2)
        total_minutos = round(sum(r["minutos"] for r in registros), 0)
        total_kg_destrio = round(sum(r["kg_destrio"] for r in registros), 1)
        total_kg_usados = round(sum(r["kg_usados"] for r in registros), 1)
        pct_destrio = round(total_kg_destrio / total_kg_usados * 100, 1) if total_kg_usados > 0 else 0
        
        # By producto
        por_producto = {}
        for r in registros:
            p = r["producto"]
            if p not in por_producto:
                por_producto[p] = {"coste": 0, "minutos": 0, "palets": 0, "kg_destrio": 0}
            por_producto[p]["coste"] += r["coste"]
            por_producto[p]["minutos"] += r["minutos"]
            por_producto[p]["palets"] += 1
            por_producto[p]["kg_destrio"] += r["kg_destrio"]
        
        # By linea
        por_linea = {}
        for r in registros:
            l = "L" + str(r["linea"])
            if l not in por_linea:
                por_linea[l] = {"coste": 0, "minutos": 0, "palets": 0}
            por_linea[l]["coste"] += r["coste"]
            por_linea[l]["minutos"] += r["minutos"]
            por_linea[l]["palets"] += 1

        return jsonify({
            "ok": True,
            "precio_hora": precio_hora,
            "registros": registros,
            "resumen": {
                "total_coste": total_coste,
                "total_minutos": int(total_minutos),
                "total_kg_usados": total_kg_usados,
                "total_kg_destrio": total_kg_destrio,
                "pct_destrio": pct_destrio,
                "num_palets": len(registros)
            },
            "por_producto": por_producto,
            "por_linea": por_linea
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


# ===== CONTROL MANIPULADO =====
MANIPULADO_SHEET = "Manipulado"

def ensure_manipulado_sheet(service):
    spreadsheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    sheet_names = [s["properties"]["title"] for s in spreadsheet["sheets"]]
    if MANIPULADO_SHEET not in sheet_names:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": MANIPULADO_SHEET}}}]}
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{MANIPULADO_SHEET}!A1",
            valueInputOption="RAW",
            body={"values": [["ID","Fecha","Pedido","Producto","Cliente","Palet","Bultos","Linea","Estado","Inicio","Fin","Minutos","Personas","Observaciones"]]}
        ).execute()

@app.route("/api/manipulado/cola", methods=["GET"])
def manipulado_cola():
    try:
        service = get_sheets_service()
        ensure_manipulado_sheet(service)
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{MANIPULADO_SHEET}!A:N"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return jsonify({"ok": True, "items": []})
        hoy = datetime.now().strftime("%d/%m/%Y")
        items = []
        for i, row in enumerate(rows[1:], 2):
            if len(row) < 9:
                continue
            estado = row[8] if len(row) > 8 else ""
            fecha = row[1] if len(row) > 1 else ""
            if fecha != hoy and estado == "finalizado":
                continue
            if estado == "anulado":
                continue
            items.append({
                "row": i,
                "id": row[0] if len(row) > 0 else "",
                "fecha": fecha,
                "pedido": row[2] if len(row) > 2 else "",
                "producto": row[3] if len(row) > 3 else "",
                "cliente": row[4] if len(row) > 4 else "",
                "palet": row[5] if len(row) > 5 else "",
                "bultos": row[6] if len(row) > 6 else "",
                "linea": row[7] if len(row) > 7 else "",
                "estado": estado,
                "inicio": row[9] if len(row) > 9 else "",
                "fin": row[10] if len(row) > 10 else "",
                "minutos": row[11] if len(row) > 11 else "",
                "personas": row[12] if len(row) > 12 else "",
                "observaciones": row[13] if len(row) > 13 else "",
            })
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "items": []})

@app.route("/api/manipulado/asignar", methods=["POST"])
def manipulado_asignar():
    try:
        data = request.get_json()
        service = get_sheets_service()
        ensure_manipulado_sheet(service)
        item_id = str(uuid.uuid4())[:8].upper()
        fecha = datetime.now().strftime("%d/%m/%Y")
        row = [
            item_id, fecha,
            str(data.get("pedido", "")),
            str(data.get("producto", "")),
            str(data.get("cliente", "")),
            str(data.get("palet", "")),
            str(data.get("bultos", "")),
            str(data.get("linea", "")),
            "espera",
            "", "", "", "",
            str(data.get("observaciones", ""))
        ]
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{MANIPULADO_SHEET}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]}
        ).execute()
        return jsonify({"ok": True, "id": item_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/manipulado/estado", methods=["POST"])
def manipulado_estado():
    try:
        data = request.get_json()
        service = get_sheets_service()
        row_num = int(data.get("row"))
        estado = str(data.get("estado", ""))
        updates = [{"range": f"{MANIPULADO_SHEET}!I{row_num}", "values": [[estado]]}]
        if estado == "en_curso" and data.get("inicio"):
            updates.append({"range": f"{MANIPULADO_SHEET}!J{row_num}", "values": [[data["inicio"]]]})
        if data.get("personas"):
            updates.append({"range": f"{MANIPULADO_SHEET}!M{row_num}", "values": [[str(data["personas"])]]})
        if estado == "finalizado":
            updates.append({"range": f"{MANIPULADO_SHEET}!J{row_num}", "values": [[data.get("inicio","")]]})
            updates.append({"range": f"{MANIPULADO_SHEET}!K{row_num}", "values": [[data.get("fin","")]]})
            updates.append({"range": f"{MANIPULADO_SHEET}!L{row_num}", "values": [[str(data.get("minutos",""))]]})
            updates.append({"range": f"{MANIPULADO_SHEET}!M{row_num}", "values": [[str(data.get("personas",""))]]})
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "RAW", "data": updates}
        ).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/manipulado/eliminar", methods=["POST"])
def manipulado_eliminar():
    try:
        data = request.get_json()
        service = get_sheets_service()
        row_num = int(data.get("row"))
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{MANIPULADO_SHEET}!I{row_num}",
            valueInputOption="RAW",
            body={"values": [["anulado"]]}
        ).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
