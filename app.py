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
import uuid

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
    },
    "linea2": {
        "hash": hashlib.sha256("ica2025".encode()).hexdigest(),
        "role": "linea",
        "nombre": "Linea"
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

@app.route("/api/organizador")
def organizador():
    """Devuelve pedidos agrupados por fecha de salida para la semana"""
    try:
        from datetime import timedelta
        df_pedidos, _, error = load_data()
        if error or df_pedidos is None or df_pedidos.empty:
            return jsonify({"ok": False, "error": error or "Sin datos"})

        df = df_pedidos.copy()

        def find_col(df, keywords):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in str(col).lower():
                        return col
            return None

        col_pedido   = find_col(df, ['pedido'])
        col_producto = find_col(df, ['producto','presentac'])
        col_cliente  = find_col(df, ['cliente'])
        col_bultos   = find_col(df, ['bultos'])
        col_palets   = find_col(df, ['palets'])
        col_fecha    = find_col(df, ['salidafecha','fecha salida','salida'])
        col_tipo     = find_col(df, ['tipo'])
        col_envase   = find_col(df, ['envase'])

        # Semana: desde hoy - 1 hasta hoy + 6
        hoy = datetime.now()
        dias = [(hoy + timedelta(days=i)) for i in range(-1, 7)]

        if col_fecha:
            df['_fecha'] = df[col_fecha].apply(lambda x: normalizar_fecha(str(x)))

        # Agrupar por fecha y pedido (un pedido puede tener varias lineas)
        pedidos_por_dia = {}
        for dia in dias:
            fecha_str = dia.strftime("%d/%m/%Y")
            pedidos_por_dia[fecha_str] = []

        if col_fecha:
            for fecha_str in pedidos_por_dia.keys():
                df_dia = df[df['_fecha'] == fecha_str]
                # Agrupar por numero de pedido
                pedidos_vistos = {}
                for _, row in df_dia.iterrows():
                    ped = str(row[col_pedido]).strip() if col_pedido else ''
                    if not ped or ped == 'nan': continue
                    if ped not in pedidos_vistos:
                        pedidos_vistos[ped] = {
                            'pedido':   ped,
                            'cliente':  str(row[col_cliente] if col_cliente else '').strip(),
                            'producto': str(row[col_producto] if col_producto else '').strip(),
                            'palets':   0,
                            'bultos':   0,
                            'tipo':     str(row[col_tipo] if col_tipo else 'PEDIDO').strip(),
                            'envase':   str(row[col_envase] if col_envase else '').strip(),
                            'fecha':    fecha_str,
                            'lineas':   0,
                        }
                    try: pedidos_vistos[ped]['palets'] += float(str(row[col_palets] if col_palets else 0).replace(',','.'))
                    except: pass
                    try: pedidos_vistos[ped]['bultos'] += int(float(str(row[col_bultos] if col_bultos else 0).replace(',','.')))
                    except: pass
                    pedidos_vistos[ped]['lineas'] += 1

                pedidos_por_dia[fecha_str] = list(pedidos_vistos.values())

        return jsonify({
            "ok": True,
            "dias": [d.strftime("%d/%m/%Y") for d in dias],
            "pedidos_por_dia": pedidos_por_dia,
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


@app.route("/api/organizador/mover", methods=["POST"])
def organizador_mover():
    """Mueve pedidos a nueva fecha en Pedidos, Cargas y Manipulado sheets"""
    try:
        data = request.json
        movimientos = data.get('movimientos', [])  # [{pedido, fecha_nueva}]
        if not movimientos:
            return jsonify({"ok": True, "actualizados": 0})

        service = get_sheets_service()

        # Load Pedidos sheet
        pedidos_data = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Pedidos!A:Z"
        ).execute().get('values', [])

        if not pedidos_data:
            return jsonify({"ok": False, "error": "Sheet Pedidos vacio"})

        headers = [str(h).lower().strip() for h in pedidos_data[0]]

        # Find column indices
        def col_idx(keywords):
            for kw in keywords:
                for i, h in enumerate(headers):
                    if kw in h:
                        return i
            return -1

        idx_pedido   = col_idx(['pedido'])
        idx_salida   = col_idx(['salida', 'fecha salida'])
        idx_llegada  = col_idx(['llegada'])

        if idx_pedido < 0 or idx_salida < 0:
            return jsonify({"ok": False, "error": "Columnas no encontradas"})

        # Build map: pedido -> nueva fecha
        mapa = {str(m['pedido']): m['fecha_nueva'] for m in movimientos}

        # Update Pedidos rows
        requests_update = []
        sheet_meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_ids = {s['properties']['title']: s['properties']['sheetId'] for s in sheet_meta['sheets']}
        ped_sheet_id = sheet_ids.get('Pedidos', 0)

        actualizados = 0
        for i, row in enumerate(pedidos_data[1:], start=1):
            if len(row) <= idx_pedido: continue
            ped = str(row[idx_pedido]).strip()
            if ped in mapa:
                nueva_fecha = mapa[ped]
                # Update fecha salida
                requests_update.append({
                    "updateCells": {
                        "range": {
                            "sheetId": ped_sheet_id,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": idx_salida,
                            "endColumnIndex": idx_salida + 1
                        },
                        "rows": [{"values": [{"userEnteredValue": {"stringValue": nueva_fecha}}]}],
                        "fields": "userEnteredValue"
                    }
                })
                actualizados += 1

        if requests_update:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": requests_update}
            ).execute()

        return jsonify({"ok": True, "actualizados": actualizados, "movimientos": len(movimientos)})

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


@app.route("/api/planificacion-diaria")
def planificacion_diaria():
    try:
        fecha = request.args.get('fecha', '')
        try:
            merma = float(str(request.args.get('merma', 12)).replace('%','')) / 100
        except:
            merma = 0.12
        hora_inicio = request.args.get('hora_inicio', '7:00')

        if not fecha:
            from datetime import timedelta
            manana = datetime.now() + timedelta(days=1)
            fecha = manana.strftime("%d/%m/%Y")

        df_pedidos, _, error = load_data()
        if error or df_pedidos is None or df_pedidos.empty:
            return jsonify({"ok": False, "error": error or "Sin datos"})

        df = df_pedidos.copy()

        def find_col(df, keywords):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in str(col).lower():
                        return col
            return None

        col_fecha    = find_col(df, ['salidafecha','fecha salida','salida'])
        col_pedido   = find_col(df, ['pedido'])
        col_producto = find_col(df, ['producto','presentac'])
        col_cliente  = find_col(df, ['cliente'])
        col_bultos   = find_col(df, ['bultos'])
        col_palets   = find_col(df, ['palets'])
        col_envase   = find_col(df, ['envase'])
        col_kilos    = find_col(df, ['kilos'])
        col_bul_plt  = find_col(df, ['bul/plt','bul_plt','bultos/palet'])

        def to_num(val, default=0):
            if val is None: return default
            s = str(val).strip().replace(',','.').replace(' ','')
            if s in ('', 'nan', 'None', 'undefined', '-'): return default
            try: return float(s)
            except: return default

        if col_fecha:
            df['_fecha'] = df[col_fecha].apply(lambda x: normalizar_fecha(str(x)))
            df_dia = df[df['_fecha'] == fecha].copy()
        else:
            df_dia = df.copy()

        if df_dia.empty:
            return jsonify({"ok": True, "fecha": fecha, "pedidos": [], "resumen": {}, "por_producto": []})

        # Sort by pedido
        if col_pedido:
            df_dia['_ped_sort'] = df_dia[col_pedido].apply(lambda x: int(str(x)) if str(x).isdigit() else 0)
            df_dia = df_dia.sort_values('_ped_sort')

        # ESTANDARES de rendimiento (ya en el JS, los duplicamos aqui)
        ESTANDARES_PY = {
            "PALERMO MIX 12X350": {"min_por_bulto": 0.808, "personas_media": 11.0},
            "PALERMO ROJO 10X500": {"min_por_bulto": 0.851, "personas_media": 11.2},
            "PEPINO ALMERIA": {"min_por_bulto": 0.379, "personas_media": 11.8},
            "CALIFORNIA ROJO 5KG": {"min_por_bulto": 0.346, "personas_media": 14.2},
            "PICANTE MIX 10X50": {"min_por_bulto": 0.46, "personas_media": 8.7},
            "PEPINO MINI 12X280": {"min_por_bulto": 0.717, "personas_media": 6.8},
            "CALABACIN": {"min_por_bulto": 0.575, "personas_media": 9.2},
            "PALERMO 10X200": {"min_por_bulto": 0.605, "personas_media": 10.8},
            "PEPINO MINI 8X3PZ": {"min_por_bulto": 0.456, "personas_media": 7.1},
            "TOMATE RAMA 10X500": {"min_por_bulto": 0.712, "personas_media": 12.8},
        }

        def buscar_estandar(producto):
            prod_up = str(producto).upper().strip()
            for key, val in ESTANDARES_PY.items():
                if key in prod_up or prod_up.startswith(key[:8]):
                    return val
            return {"min_por_bulto": 0.6, "personas_media": 11.0}  # default

        pedidos = []
        total_bultos = 0
        total_palets = 0
        total_kg_neto = 0
        total_kg_bruto = 0
        total_horas_persona = 0
        total_personas_rec = 0

        # Parse hora inicio
        try:
            h_ini, m_ini = [int(x) for x in hora_inicio.split(':')]
        except:
            h_ini, m_ini = 7, 0
        minutos_acumulados = 0

        for _, row in df_dia.iterrows():
            bultos   = to_num(row[col_bultos] if col_bultos and col_bultos in row.index else 0)
            palets   = to_num(row[col_palets] if col_palets and col_palets in row.index else 0)
            kg_total = to_num(row[col_kilos] if col_kilos and col_kilos in row.index else 0)
            producto = str(row[col_producto] if col_producto else '')
            envase   = str(row[col_envase]   if col_envase  else '')
            bul_plt  = to_num(row[col_bul_plt] if col_bul_plt and col_bul_plt in row.index else 0)

            # Kg por bulto
            if bultos > 0 and kg_total > 0:
                kg_por_bulto = kg_total / bultos
            else:
                kg_por_bulto = 0

            kg_neto  = kg_total
            kg_bruto = kg_neto * (1 + merma) if kg_neto > 0 else 0

            # Estándares
            est = buscar_estandar(producto)
            min_por_bulto  = est['min_por_bulto']
            personas_media = est['personas_media']

            minutos_totales = bultos * min_por_bulto if bultos > 0 else 0
            horas_estimadas = minutos_totales / personas_media / 60 if personas_media > 0 else 0
            personas_rec    = round(personas_media)

            # Hora inicio este pedido
            hora_ini_min = h_ini * 60 + m_ini + minutos_acumulados
            hora_ini_str = '%02d:%02d' % (hora_ini_min // 60, hora_ini_min % 60)
            hora_fin_min = hora_ini_min + int(minutos_totales / personas_media) if personas_media > 0 else hora_ini_min + 60
            hora_fin_str = '%02d:%02d' % (hora_fin_min // 60, hora_fin_min % 60)
            minutos_acumulados += int(minutos_totales / personas_media) if personas_media > 0 else 60

            total_bultos       += bultos
            total_palets       += palets
            total_kg_neto      += kg_neto
            total_kg_bruto     += kg_bruto
            total_horas_persona+= minutos_totales / 60
            total_personas_rec += personas_rec

            # Material: etiquetas = bultos (1 por caja), zunchos estimados
            etiquetas = int(bultos)
            zunchos   = int(palets * 4) if palets > 0 else 0

            pedidos.append({
                "pedido":          str(row[col_pedido] if col_pedido else ''),
                "producto":        producto,
                "cliente":         str(row[col_cliente] if col_cliente else ''),
                "bultos":          int(bultos),
                "palets":          palets,
                "envase":          envase,
                "kg_neto":         round(kg_neto, 1),
                "kg_bruto":        round(kg_bruto, 1),
                "kg_por_bulto":    round(kg_por_bulto, 3),
                "personas_rec":    personas_rec,
                "horas_estimadas": round(horas_estimadas, 1),
                "hora_inicio":     hora_ini_str,
                "hora_fin":        hora_fin_str,
                "etiquetas":       etiquetas,
                "zunchos":         zunchos,
            })

        # Resumen por producto
        por_producto = {}
        for p in pedidos:
            prod = p['producto']
            if prod not in por_producto:
                por_producto[prod] = {"producto": prod, "bultos": 0, "palets": 0, "kg_neto": 0, "kg_bruto": 0, "personas_rec": p['personas_rec'], "horas": 0, "etiquetas": 0}
            por_producto[prod]['bultos']    += p['bultos']
            por_producto[prod]['palets']    += p['palets']
            por_producto[prod]['kg_neto']   += p['kg_neto']
            por_producto[prod]['kg_bruto']  += p['kg_bruto']
            por_producto[prod]['horas']     += p['horas_estimadas']
            por_producto[prod]['etiquetas'] += p['etiquetas']

        por_producto_list = sorted(por_producto.values(), key=lambda x: x['bultos'], reverse=True)

        return jsonify({
            "ok": True,
            "fecha": fecha,
            "merma_pct": int(merma * 100),
            "hora_inicio": hora_inicio,
            "pedidos": pedidos,
            "por_producto": por_producto_list,
            "resumen": {
                "total_pedidos":  len(pedidos),
                "total_bultos":   int(total_bultos),
                "total_palets":   round(total_palets, 1),
                "total_kg_neto":  round(total_kg_neto, 1),
                "total_kg_bruto": round(total_kg_bruto, 1),
                "total_etiquetas":int(total_bultos),
                "total_zunchos":  int(total_palets * 4),
                "horas_trabajo":  round(total_horas_persona / max(total_personas_rec / len(pedidos), 1), 1) if pedidos else 0,
            }
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


@app.route("/api/resumen-diario")
def resumen_diario():
    try:
        fecha = request.args.get('fecha', datetime.now().strftime("%d/%m/%Y"))
        df_pedidos, _, error = load_data()
        if error or df_pedidos is None or df_pedidos.empty:
            return jsonify({"ok": False, "error": error or "Sin datos"})

        df = df_pedidos.copy()

        def find_col(df, keywords):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in str(col).lower():
                        return col
            return None

        col_fecha    = find_col(df, ['salidafecha','fecha salida','salida'])
        col_pedido   = find_col(df, ['pedido'])
        col_producto = find_col(df, ['producto','presentac'])
        col_cliente  = find_col(df, ['cliente'])
        col_bultos   = find_col(df, ['bultos'])
        col_palets   = find_col(df, ['palets'])
        col_envase   = find_col(df, ['envase','caja'])

        if col_fecha:
            df['_fecha'] = df[col_fecha].apply(lambda x: normalizar_fecha(str(x)))
            df_dia = df[df['_fecha'] == fecha]
        else:
            df_dia = df

        # Sort by pedido
        if col_pedido:
            df_dia = df_dia.copy()
            df_dia['_ped_sort'] = df_dia[col_pedido].apply(lambda x: int(str(x)) if str(x).isdigit() else 0)
            df_dia = df_dia.sort_values('_ped_sort')

        pedidos = []
        for _, row in df_dia.iterrows():
            pedidos.append({
                "pedido":   str(row[col_pedido])   if col_pedido   else "",
                "producto": str(row[col_producto]) if col_producto else "",
                "cliente":  str(row[col_cliente])  if col_cliente  else "",
                "bultos":   str(row[col_bultos])   if col_bultos   else "",
                "palets":   str(row[col_palets])   if col_palets   else "",
                "envase":   str(row[col_envase])   if col_envase   else "",
            })

        return jsonify({"ok": True, "fecha": fecha, "pedidos": pedidos})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/dashboard")
def dashboard():
    try:
        df_pedidos, df_cargas, error = load_data()
        if error or df_pedidos is None or df_pedidos.empty:
            return jsonify({"ok": False, "error": error or "Sin datos"})

        hoy = datetime.now().strftime("%d/%m/%Y")
        df = df_pedidos.copy()

        # Find column names (flexible)
        def find_col(df, keywords):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in str(col).lower():
                        return col
            return None

        col_fecha   = find_col(df, ['salidafecha','fecha salida','salida','fecha'])
        col_palets  = find_col(df, ['palets','palet'])
        col_bultos  = find_col(df, ['bultos','bulto'])
        col_cliente = find_col(df, ['cliente'])
        col_producto= find_col(df, ['producto','presentac'])
        col_pedido  = find_col(df, ['pedido'])

        # Normalize palets/bultos to numeric
        def to_num(series):
            return series.apply(lambda x: float(str(x).replace(',','.')) if str(x).replace(',','').replace('.','').strip().isdigit() else 0)

        if col_palets:  df['_palets']  = to_num(df[col_palets])
        else:           df['_palets']  = 0
        if col_bultos:  df['_bultos']  = to_num(df[col_bultos])
        else:           df['_bultos']  = 0

        # Normalize dates
        if col_fecha:
            df['_fecha'] = df[col_fecha].apply(lambda x: normalizar_fecha(str(x)))
        else:
            df['_fecha'] = ''

        # Get period filter from query param
        periodo = request.args.get('periodo', 'anyo')
        now = datetime.now()

        if periodo == 'hoy':
            mask = df['_fecha'] == hoy
        elif periodo == 'semana':
            lunes = (now - __import__('datetime').timedelta(days=now.weekday())).strftime("%d/%m/%Y")
            mask = df['_fecha'] >= lunes
        elif periodo == 'mes':
            mes_actual = now.strftime("%m/%Y")
            mask = df['_fecha'].str.endswith(mes_actual)
        else:  # anyo
            anyo_actual = now.strftime("%Y")
            mask = df['_fecha'].str.endswith(anyo_actual)

        df_period = df[mask] if col_fecha else df

        # --- RESUMEN HOY ---
        df_hoy = df[df['_fecha'] == hoy]
        resumen_hoy = {
            "pedidos": int(df_hoy[col_pedido].nunique()) if col_pedido else 0,
            "palets":  round(float(df_hoy['_palets'].sum()), 2),
            "bultos":  int(df_hoy['_bultos'].sum()),
        }

        # --- TOP CLIENTES ---
        top_clientes = []
        if col_cliente:
            tc = df_period.groupby(col_cliente)['_palets'].sum().sort_values(ascending=False).head(10)
            top_clientes = [{"cliente": str(k), "palets": round(float(v), 1)} for k, v in tc.items() if str(k).strip() and str(k) != 'nan']

        # --- TOP PRODUCTOS ---
        top_productos = []
        if col_producto:
            tp = df_period.groupby(col_producto)['_palets'].sum().sort_values(ascending=False).head(10)
            top_productos = [{"producto": str(k), "palets": round(float(v), 1)} for k, v in tp.items() if str(k).strip() and str(k) != 'nan']

        # --- PEDIDOS POR SEMANA ---
        pedidos_semana = []
        if col_fecha:
            import datetime as dt2
            df_anyo = df[df['_fecha'].str.endswith(now.strftime("%Y"))]
            df_anyo = df_anyo[df_anyo['_fecha'].str.match(r'\d{2}/\d{2}/\d{4}')]
            df_anyo['_dt'] = df_anyo['_fecha'].apply(lambda x: dt2.datetime.strptime(x, "%d/%m/%Y") if x else None)
            df_anyo = df_anyo.dropna(subset=['_dt'])
            df_anyo['_semana'] = df_anyo['_dt'].apply(lambda x: f"S{x.isocalendar()[1]:02d}")
            sw = df_anyo.groupby('_semana')['_palets'].sum().sort_index()
            pedidos_semana = [{"semana": str(k), "palets": round(float(v), 1)} for k, v in sw.items()]

        # --- TOTALES PERIODO ---
        totales = {
            "pedidos": int(df_period[col_pedido].nunique()) if col_pedido else 0,
            "palets":  round(float(df_period['_palets'].sum()), 1),
            "bultos":  int(df_period['_bultos'].sum()),
        }

        return jsonify({
            "ok": True,
            "periodo": periodo,
            "hoy": hoy,
            "resumen_hoy": resumen_hoy,
            "totales": totales,
            "top_clientes": top_clientes,
            "top_productos": top_productos,
            "pedidos_semana": pedidos_semana,
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


@app.route("/api/pedidos")
def api_pedidos():
    try:
        df_pedidos, _, error = load_data()
        if error:
            return jsonify({"ok": False, "error": error})
        if df_pedidos is None or df_pedidos.empty:
            return jsonify({"ok": True, "pedidos": [], "dias": []})

        df = df_pedidos.copy()
        cols = list(df.columns)

        def exact_col(names):
            """Find column by exact name match first, then partial"""
            for name in names:
                if name in cols:
                    return name
            for name in names:
                for col in cols:
                    if name.lower() == str(col).lower().strip():
                        return col
            for name in names:
                for col in cols:
                    if name.lower() in str(col).lower():
                        return col
            return None

        # Use exact names from the sheet
        col_fecha    = exact_col(['Fecha Salida', 'Fecha_Salida', 'FechaSalida', 'salida'])
        col_pedido   = exact_col(['Pedido'])
        col_producto = exact_col(['Producto', 'Presentacion'])
        col_cliente  = exact_col(['Cliente'])
        col_bultos   = exact_col(['Bultos'])          # exact - not Bul/Plt
        col_palets   = exact_col(['Palets'])           # exact
        col_tipo     = exact_col(['Tipo'])
        col_envase   = exact_col(['Envase'])
        col_tipo_pal = exact_col(['Tipo Palet'])
        col_kilos    = exact_col(['Kilos'])
        col_lote     = exact_col(['Lote'])
        col_tcult    = exact_col(['T.Cult', 'TCult', 'Ecologico', 'Ecol'])
        col_dest     = exact_col(['Destino'])
        col_llegada  = exact_col(['Fecha Llegada', 'Llegada'])

        # Normalize fecha
        if col_fecha:
            df['_fecha'] = df[col_fecha].apply(lambda x: normalizar_fecha(str(x)))
        else:
            df['_fecha'] = ''

        # Get sorted unique dates
        from datetime import timedelta
        hoy = datetime.now()
        rango = set((hoy + timedelta(days=i)).strftime("%d/%m/%Y") for i in range(-2, 14))
        fechas_datos = set(f for f in df['_fecha'].unique() if f and len(f) == 10)
        dias = sorted(rango | fechas_datos, key=lambda x: (x[6:], x[3:5], x[:2]))

        def sv(col):
            if not col: return ''
            return lambda row: str(row.get(col, '') or '').strip()

        def nv(col):
            if not col: return lambda row: 0
            def get(row):
                try: return float(str(row.get(col, 0) or 0).replace(',', '.'))
                except: return 0
            return get

        get_pedido   = sv(col_pedido)
        get_producto = sv(col_producto)
        get_cliente  = sv(col_cliente)
        get_bultos   = nv(col_bultos)
        get_palets   = nv(col_palets)
        get_tipo     = sv(col_tipo)
        get_envase   = sv(col_envase)
        get_tpal     = sv(col_tipo_pal)
        get_kilos    = nv(col_kilos)
        get_lote     = sv(col_lote)
        get_tcult    = sv(col_tcult)
        get_dest     = sv(col_dest)
        get_llegada  = sv(col_llegada)

        pedidos = []
        for _, row in df.iterrows():
            row = row.to_dict()
            ped = get_pedido(row)
            if not ped or ped == 'nan': continue
            pedidos.append({
                'pedido':       ped,
                'producto':     get_producto(row),
                'cliente':      get_cliente(row),
                'fecha_salida': df.at[_, '_fecha'] if col_fecha else '',
                'palets':       get_palets(row),
                'bultos':       int(get_bultos(row)),
                'kilos':        get_kilos(row),
                'tipo':         get_tipo(row) or 'PEDIDO',
                'envase':       get_envase(row),
                'tipo_palet':   get_tpal(row),
                'lote':         get_lote(row),
                't_cult':       get_tcult(row),
                'destino':      get_dest(row),
                'fecha_llegada':get_llegada(row),
            })

        return jsonify({
            "ok": True,
            "pedidos": pedidos,
            "dias": dias,
            "debug": {
                "cols": cols,
                "col_fecha": col_fecha,
                "col_bultos": col_bultos,
                "col_palets": col_palets,
                "total": len(pedidos),
            }
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


@app.route("/api/debug-cols")
def debug_cols():
    try:
        df_pedidos, df_cargas, error = load_data()
        if error:
            return jsonify({"ok": False, "error": error})
        return jsonify({
            "ok": True,
            "pedidos_cols": list(df_pedidos.columns) if df_pedidos is not None else [],
            "pedidos_sample": df_pedidos.head(2).to_dict(orient='records') if df_pedidos is not None else [],
            "cargas_cols": list(df_cargas.columns) if df_cargas is not None else [],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


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
        "fecha": hoy,
        "version": "2.1-debug"
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
        pedido_num = str(row.get("Pedido", ""))
        def _s(col_name):
            for col in row.index if hasattr(row,'index') else []:
                if col_name.lower() in str(col).lower():
                    v = row[col]
                    return str(v) if v and str(v) != "nan" else ""
            return str(row.get(col_name, "") or "")
        entry = {
            "pedido":        pedido_num,
            "cliente":       str(row.get("Cliente", "") or ""),
            "producto":      str(row.get("Producto", "") or ""),
            "bultos":        str(row.get("Bultos", "") or ""),
            "palets":        str(row.get("Palets", "") or ""),
            "transportista": str(row.get("Transportista", "") or ""),
            "matricula":     str(row.get("Matricula", "") or ""),
            "lote":          str(row.get("Lote", "") or ""),
            "envase":        str(row.get("Envase", "") or ""),
            "tipo":          str(row.get("Tipo", "") or ""),
            "destino":       str(row.get("Destino", "") or ""),
            "referencia":    str(row.get("Referencia", "") or ""),
            "tipo_palet":    str(row.get("Tipo Palet", "") or ""),
            "kilos":         str(row.get("Kilos", "") or ""),
            "fecha_pedido":  str(row.get("Fecha Pedido", "") or ""),
            "fecha_llegada": str(row.get("Fecha Llegada", "") or ""),
            "modificacion":  False
        }
        # Detectar modificacion: mismo pedido+producto ya existe en este dia
        if fecha in dias:
            for prev in dias[fecha]:
                if prev["pedido"] == pedido_num and prev["producto"] == entry["producto"]:
                    prev["modificacion"] = True
                    entry["modificacion"] = True
                    break
        dias[fecha].append(entry)

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
                estados.append({"pedido": row[1], "estado": valor, "tipo": tipo})
            elif tipo in ("palet_man_fin", "traza_palet"):
                estados.append({"pedido": row[1], "estado": tipo, "valor": valor, "tipo": tipo})
            elif tipo == "palet_activo":
                import json as json_mod
                try:
                    palets.append(json_mod.loads(valor))
                except:
                    pass
            elif tipo == "palet_finalizado":
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
    cargas_info = {}  # key: pedido|producto
    if not df_cargas.empty:
        # Normalize column names - handle accents and variations
        col_map = {}
        for col in df_cargas.columns:
            cl = col.lower().strip()
            if 'matricul' in cl or 'matrícul' in cl:
                col_map['matricula'] = col
            elif 'transportist' in cl:
                col_map['transportista'] = col
            elif 'observ' in cl:
                col_map['observaciones'] = col
            elif 'mercanc' in cl or 'producto' in cl:
                col_map['producto'] = col
            elif 'tel' in cl or 'phone' in cl:
                col_map['telefono'] = col

        for _, row in df_cargas.iterrows():
            pedido = str(row.get("Pedido", "") or "").strip()
            if not pedido or pedido == "nan":
                continue
            # Get producto from cargas sheet (may be under Mercancia column)
            prod_col = col_map.get('producto', 'Producto')
            producto = str(row.get(prod_col, "") or "").strip()
            mat_col = col_map.get('matricula', 'Matricula')
            tra_col = col_map.get('transportista', 'Transportista')
            obs_col = col_map.get('observaciones', 'Observaciones')
            tel_col = col_map.get('telefono', '')
            matricula = str(row.get(mat_col, "") or "").strip()
            transportista = str(row.get(tra_col, "") or "").strip()
            obs = str(row.get(obs_col, "") or "").strip()
            telefono = str(row.get(tel_col, "") or "").strip() if tel_col else ""
            entry = {
                "matricula": "" if matricula == "nan" else matricula,
                "transportista": "" if transportista == "nan" else transportista,
                "observaciones": "" if obs == "nan" else obs,
                "telefono": "" if telefono == "nan" else telefono,
            }
            # Store by pedido+producto key AND by pedido alone (last wins for pedido-only)
            key = pedido + "|" + producto
            cargas_info[key] = entry
            # Also store by pedido for fallback (first match with data wins)
            if pedido not in cargas_info or (not cargas_info[pedido].get("matricula") and entry.get("matricula")):
                cargas_info[pedido] = entry

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
        producto_key = pedido + "|" + str(row.get("Producto", "") or "").strip()
        extra = cargas_info.get(producto_key) or cargas_info.get(pedido, {})
        items.append({
            "pedido": pedido,
            "cliente": str(row.get("Cliente", "") or "").strip(),
            "producto": str(row.get("Producto", "") or "").strip(),
            "bultos": str(row.get("Bultos", "") or "").strip(),
            "palets": str(row.get("Palets", "") or "").strip(),
            "matricula": extra.get("matricula", ""),
            "transportista": extra.get("transportista", str(row.get("Transportista", "") or "").strip()),
            "observaciones": extra.get("observaciones", ""),
            "telefono": extra.get("telefono", ""),
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
            # Clean: remove proveedor name prefix if present (e.g. "ICA PRODUKT 2025, S.L. Producto")
            if resultado.get('producto'):
                prod = resultado['producto']
                # If starts with ICA, remove up to first product word (after S.L. or similar)
                m_ica = re.search(r'(?:S\.L\.|S\.A\.|S\.L|ICA[^,]+,\s*S\.L\.?)\s+(.+)', prod)
                if m_ica:
                    resultado['producto'] = m_ica.group(1).strip()

        m4 = re.search(r'BULTOS/PALET:\s*(\d+)', line)
        if m4:
            resultado['bultos_palet'] = int(m4.group(1))

        m5 = re.search(r'MARCA:\s*(.+)', line)
        if m5:
            resultado['marca'] = m5.group(1).strip()

        # Pedido row: 4 digits, carga, day name, date, palets, bultos, lote
        row = re.match(r'^(\d{4})\s+(\d+)\s+\S+\s+(\d{2}/\d{2}/\d{4})\s+([\d,\.]+)\s+(\d+)\s+(.+)$', line.strip())
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



def limpiar_doble(texto):
    """Fix doubled characters: AALLMMEERRIIAA -> ALMERIA, ,, -> ,  .. -> ."""
    if not texto: return ""
    t = str(texto)
    resultado = []
    i = 0
    while i < len(t):
        ch = t[i]
        if i + 1 < len(t) and ch == t[i+1]:
            # Always deduplicate doubled chars including punctuation
            resultado.append(ch)
            i += 2
        else:
            resultado.append(ch)
            i += 1
    # Clean up any remaining double slashes in dates
    import re as _re
    r = ''.join(resultado)
    r = _re.sub(r'/{2,}', '/', r)
    return r.strip()

def limpiar_fecha_doble(fecha):
    """Fix doubled date: 0033//0033//22002266 -> 03/03/2026"""
    import re
    limpia = limpiar_doble(fecha)
    m = re.search(r'(\d{2}/\d{2}/\d{4})', limpia)
    return m.group(1) if m else limpia.strip()

def tiene_dobles(texto):
    """Detect if text has doubled characters like AALLMMEERRIIAA"""
    if not texto or len(texto) < 6:
        return False
    t = str(texto)
    doubles = sum(1 for i in range(0, len(t)-1, 2) if t[i] == t[i+1] and t[i].isalpha())
    return doubles >= 3

def limpiar_si_doble(texto):
    """Only deduplicate if text actually has doubled chars"""
    if not texto:
        return ""
    t = str(texto).strip()
    if tiene_dobles(t):
        return limpiar_doble(t)
    return t

def primera_linea(celda):
    """Get first non-empty line from a merged cell"""
    if not celda:
        return ""
    lineas = str(celda).split("\n")
    for l in lineas:
        l = l.strip()
        if l and len(l) > 1:
            return l
    return ""

def parse_pedido_proveedor_pdf(file_bytes):
    """Parse Pedido a Proveedor PDF - ICA format, maneja texto triplicado"""
    import io, re
    pedidos = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    def es_corrompida(s):
        if len(s) < 6: return False
        pares = sum(1 for i in range(len(s)-1) if s[i]==s[i+1])
        return pares > len(s) * 0.25

    def limpiar(s):
        if not s: return s
        r = re.sub(r'(.)\1{2}', r'\1', s)
        r = re.sub(r'(.)\1', r'\1', r)
        return r

    # Limpiar todas las lineas
    lines_raw = text.split('\n')
    lines = [limpiar(l) if es_corrompida(l) else l for l in lines_raw]

    # Extraer cabecera
    pedido_num = None
    fecha_pedido = None
    fecha_salida = None
    cliente = None
    referencia = None

    for line in lines:
        line = line.strip()
        if not line: continue

        m = re.search(r'Pedido[:\s]+(\d+)', line)
        if m and not pedido_num:
            pedido_num = m.group(1)

        fechas = re.findall(r'\d{2}/\d{2}/\d{4}', line)
        if fechas and not fecha_pedido:
            fecha_pedido = fechas[0]
            if len(fechas) > 1: fecha_salida = fechas[1]

        # Referencia/Bestellnummer (linea con solo ref y fechas)
        if not referencia and fecha_pedido and re.match(r'^[A-Z0-9]{4,}\s', line):
            ref_m = re.match(r'^([A-Z]{1,3}\d{4,}|\d{6,})', line)
            if ref_m: referencia = ref_m.group(1)

    # Cliente: primera linea en mayusculas que no sea ICA/PASEO/PEDIDO
    SKIP = ['ICA', 'PASEO', 'CALLE', 'CIF', 'PEDIDO', 'REVISADO', 'TOTAL',
            'PRESENTACION', 'BESTELLNUMMER', 'INSTRUCCIONES', 'ADJUNTO', 'CAMBIOS']
    for line in lines:
        line = line.strip()
        if not line or len(line) < 4: continue
        if not re.match(r'^[A-Z]', line): continue
        if any(sk in line.upper() for sk in SKIP): continue
        if re.match(r'^\d', line): continue
        if re.search(r'\d{2}/\d{2}/\d{4}', line): continue
        cliente = line
        break

    # Extraer lineas de producto
    TCULTS = ['ECOLOGICO', 'CONVENCIONAL']
    CC_VALS = ['I', 'II', 'III', 'IV']
    PALETS_T = ['EURO RET.', 'EURO RET', 'CHEP', 'INDUST']

    for line in lines:
        line = line.strip()
        if not line: continue

        tcult = None
        for tc in TCULTS:
            if tc in line.upper():
                tcult = tc
                break
        if not tcult: continue

        # Nombre producto: texto antes del T.Cult
        idx_tc = line.upper().find(tcult)
        nombre = line[:idx_tc].strip()
        if len(nombre) < 3: continue
        if re.match(r'^[\d.,/ ]+$', nombre): continue

        resto = line[idx_tc:].strip()

        # Lote
        lote_m = re.search(r'\b(\d{3,5})\b', resto)
        lote = lote_m.group(1) if lote_m else None

        # C/C
        cc = None
        for cv in CC_VALS:
            if re.search(r'\b' + cv + r'\b', resto):
                cc = cv
                break

        # Tipo palet
        tipo_palet = None
        for tp in PALETS_T:
            if tp in resto.upper():
                tipo_palet = tp
                break

        # Envase (BLL, IFCO, CARTON, EPS, CAJA)
        envase = None
        env_m = re.search(r'(IFCO\s+BLL\d+|BLL\d+|CARTON[^\s]*|EPS[^\s]*|CAJA[^\s]*)', resto, re.I)
        if env_m: envase = env_m.group(1)

        # Numeros: kilos bul_plt palets bultos precio
        # Eliminar el lote de la lista de numeros
        nums_raw = re.findall(r'(?<!\d)([\d][\d.]*(?:,[\d]+)?)(?!\d)', resto)
        nums = [n for n in nums_raw if n != lote and n != cc]

        def n(s):
            try: return float(s.replace('.','').replace(',','.'))
            except: return None

        kilos = bul_plt = palets = bultos = precio = None
        if len(nums) >= 4:
            kilos   = n(nums[0])
            bul_plt = n(nums[1])
            palets  = n(nums[2])
            bultos  = n(nums[3])
            if len(nums) > 4: precio = n(nums[4])

        pedidos.append({
            'pedido':        pedido_num,
            'cliente':       cliente or '',
            'destino':       cliente or '',
            'referencia':    referencia or '',
            'fecha_pedido':  fecha_pedido or '',
            'fecha_salida':  fecha_salida or '',
            'fecha_llegada': '',
            'producto':      nombre,
            't_cult':        tcult,
            'lote':          lote or '',
            'cc':            cc or '',
            'envase':        envase or '',
            'tipo_palet':    tipo_palet or '',
            'kilos':         kilos,
            'bul_plt':       bul_plt,
            'palets':        palets,
            'bultos':        bultos,
            'precio':        precio,
            'matricula':     '',
            'transportista': '',
            'observaciones': '',
        })

    return {'pedidos': pedidos, 'pedido': pedido_num}


def get_sheet_id(service, spreadsheet_id, sheet_name):
    """Get numeric sheetId for a named sheet"""
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get('sheets', []):
        if s['properties']['title'] == sheet_name:
            return s['properties']['sheetId']
    return 0


@app.route("/api/importar-pdf", methods=["POST"])
def importar_programa():
    """Receive PDF, parse it, insert rows into Pedidos sheet"""
    try:


        if 'pdf' not in request.files:
            return jsonify({"ok": False, "error": "No se recibio PDF"})

        file = request.files['pdf']
        file_bytes = file.read()
        # Detect PDF format
        import io as _io
        with pdfplumber.open(_io.BytesIO(file_bytes)) as _pdf:
            _text = _pdf.pages[0].extract_text() or ""

        if 'PEDIDO A PROVEEDOR' in _text or 'Pedido a Proveedor' in _text or 'Bestellnummer' in _text:
            data = parse_pedido_proveedor_pdf(file_bytes)
        else:
            data = parse_programa_pdf(file_bytes)

        if not data.get('pedidos'):
            # Return detailed debug info
            import pdfplumber as _pl2, io as _io2
            with _pl2.open(_io2.BytesIO(file_bytes)) as _pdf2:
                _text2 = _pdf2.pages[0].extract_text() or ""
                _tables2 = _pdf2.pages[0].extract_tables()
            return jsonify({
                "ok": False,
                "error": "No se encontraron pedidos en el PDF",
                "debug": data,
                "text_snippet": _text2[:300],
                "num_tables": len(_tables2),
                "table2_row1": [str(c)[:80] if c else '' for c in (_tables2[2][1] if len(_tables2)>2 and len(_tables2[2])>1 else [])],
                "formato_detectado": "pedido_proveedor" if ('PEDIDO A PROVEEDOR' in _text2 or 'Bestellnummer' in _text2) else "programa"
            })

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

        # For pedido_proveedor format, fields are per-row
        es_pedido_proveedor = data.get('tipo') == 'pedido_proveedor'
        producto = data.get('producto', '')
        cliente = data.get('cliente', '')
        marca = data.get('marca', '')
        programa = data.get('programa', '')
        bultos_palet = data.get('bultos_palet', '')

        # Exact column mapping based on sheet structure:
        # 1=Pedido 2=Tipo 3=Cliente 4=Destino 5=Referencia 6=Fecha 7=PedidoFecha
        # 8=SalidaFecha 9=Llegada 10=Producto 10=T.Cult 11=Lote 12=C/C 13=Envase
        # 14=TipoPalet 15=Kilos 16=Bul/Plt 17=Palets 18=Bultos 19=Precio
        # 20=Matricula 21=Transportista 22=TotalPalets 24=TotalBultos
        # 25=TotalImporte 26=Observaciones 27=FechaProcesado
        tipo_doc = 'PEDIDO' if es_pedido_proveedor else 'PROGRAMA'
        fecha_procesado = datetime.now().strftime('%d/%m/%Y %H:%M')

        rows_to_add = []
        for p in data['pedidos']:
            row = [''] * max(len(headers), 27)

            def set_col(name, val):
                for i, h in enumerate(headers):
                    if str(h).strip().lower() == name.lower():
                        row[i] = val
                        return
                    if name.lower() in str(h).strip().lower():
                        row[i] = val
                        return

            p_producto      = p.get('producto')      or producto
            p_cliente       = p.get('cliente')       or cliente or marca
            p_lote          = p.get('lote',          '')
            p_envase        = p.get('envase',        '')
            p_transportista = p.get('transportista', '')
            p_matricula     = p.get('matricula',     '')
            p_fecha_salida  = p.get('fecha_salida',  '')
            p_fecha_llegada = p.get('fecha_llegada', '')
            p_fecha_pedido  = p.get('fecha_pedido',  '') or p_fecha_salida
            p_destino       = p.get('destino',       '')
            p_referencia    = p.get('referencia',    '')
            p_tipo_cult     = p.get('tipo',          '')  # T.Cultivo: ECOLOGICO/CONVENCIONAL
            p_tipo_palet    = p.get('tipo_palet',    '')
            p_kilos         = p.get('kilos',         '')
            p_palets        = p.get('palets',        '')
            p_bultos        = p.get('bultos',        '')
            p_bul_plt       = data.get('bultos_palet', '')

            # Col 1: Pedido
            set_col('pedido',           p['pedido'])
            # Col 2: Tipo = PEDIDO o PROGRAMA (tipo de documento, NOT t.cultivo)
            set_col('tipo',             tipo_doc)
            # Col 3: Cliente
            set_col('cliente',          p_cliente)
            # Col 4: Destino
            set_col('destino',          p_destino or p_cliente)
            # Col 5: Referencia
            set_col('referencia',       p_referencia)
            # Col 6: Fecha (salida)
            set_col('fecha',            p_fecha_salida)
            # Col 7: PedidoFecha
            set_col('pedidofecha',      p_fecha_pedido)
            set_col('fecha pedido',     p_fecha_pedido)
            # Col 8: SalidaFecha
            set_col('salidafecha',      p_fecha_salida)
            set_col('fecha salida',     p_fecha_salida)
            set_col('salida',           p_fecha_salida)
            # Col 9: Llegada
            set_col('llegada',          p_fecha_llegada)
            set_col('fecha llegada',    p_fecha_llegada)
            # Col 10: Producto / Presentacion
            set_col('producto',         p_producto)
            set_col('presentacion',     p_producto)
            # Col 10: T.Cult (tipo cultivo: ECOLOGICO/CONVENCIONAL)
            set_col('t.cult',           p_tipo_cult)
            set_col('tcult',            p_tipo_cult)
            set_col('cultivo',          p_tipo_cult)
            # Col 11: Lote
            set_col('lote',             p_lote)
            # Col 12: C/C
            set_col('c/c',              p.get('cc', ''))
            # Col 13: Envase
            set_col('envase',           p_envase)
            # Col 14: TipoPalet
            set_col('tipopalet',        p_tipo_palet)
            set_col('tipo palet',       p_tipo_palet)
            set_col('palet',            p_tipo_palet)
            # Col 15: Kilos
            set_col('kilos',            p_kilos)
            # Col 16: Bul/Plt
            set_col('bul/plt',          p_bul_plt)
            set_col('bultos/palet',     p_bul_plt)
            # Col 17: Palets
            set_col('palets',           p_palets)
            # Col 18: Bultos
            set_col('bultos',           p_bultos)
            # Col 19: Precio
            set_col('precio',           p.get('precio', ''))
            # Col 20: Matricula
            set_col('matricula',        p_matricula)
            # Col 21: Transportista
            set_col('transportista',    p_transportista)
            # Col 22: TotalPalets
            set_col('totalpalets',      p_palets)
            set_col('total palets',     p_palets)
            # Col 24: TotalBultos
            set_col('totalbultos',      p_bultos)
            set_col('total bultos',     p_bultos)
            # Col 26: Observaciones
            set_col('observaciones',    data.get('observaciones', ''))
            # Col 27: Fecha Procesado
            set_col('fecha procesado',  fecha_procesado)
            set_col('procesado',        fecha_procesado)
            set_col('fechaprocesado',   fecha_procesado)

            rows_to_add.append(row[:len(headers)])

        # Borrar filas existentes con los mismos numeros de pedido antes de insertar
        numeros_pedido = list(set([str(r[0]) for r in rows_to_add if r[0]]))
        if numeros_pedido:
            sheet_data = service.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range="Pedidos!A:A"
            ).execute().get('values', [])
            filas_borrar = []
            for i, row_val in enumerate(sheet_data):
                if i == 0: continue  # skip header
                if row_val and str(row_val[0]).strip() in numeros_pedido:
                    filas_borrar.append(i + 1)  # 1-based
            # Borrar de abajo arriba para no desplazar indices
            if filas_borrar:
                requests_borrar = []
                for fila_idx in sorted(filas_borrar, reverse=True):
                    requests_borrar.append({
                        "deleteDimension": {
                            "range": {
                                "sheetId": get_sheet_id(service, SHEET_ID, "Pedidos"),
                                "dimension": "ROWS",
                                "startIndex": fila_idx - 1,
                                "endIndex": fila_idx
                            }
                        }
                    })
                service.spreadsheets().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={"requests": requests_borrar}
                ).execute()

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
            "pedidos": data['pedidos'],
            "headers_sheet": headers,
            "sample_row": rows_to_add[0] if rows_to_add else []
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})



@app.route("/api/guardar-lote", methods=["POST"])
def guardar_lote():
    """Guardar lote + kg destrio en hoja Confecciones"""
    try:
        data = request.get_json()
        lote    = str(data.get("lote", "")).strip()
        destrio = str(data.get("destrio", "0")).strip()
        usuario = str(data.get("usuario", "Linea")).strip()
        fecha   = str(data.get("fecha", datetime.now().strftime("%d/%m/%Y"))).strip()

        if not lote:
            return jsonify({"ok": False, "error": "Lote requerido"})

        service = get_sheets_service()

        # Get headers from Confecciones sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range="Confecciones!1:1"
        ).execute()
        headers = result.get("values", [[]])[0]

        def col_idx(name):
            for i, h in enumerate(headers):
                if name.lower() in str(h).lower():
                    return i
            return -1

        row = [""] * max(len(headers), 10)
        ts = datetime.now().strftime("%d/%m/%Y %H:%M")

        def setc(name, val):
            i = col_idx(name)
            if i >= 0: row[i] = val

        setc("fecha",   fecha)
        setc("hora",    datetime.now().strftime("%H:%M"))
        setc("lote",    lote)
        setc("destrio", destrio)
        setc("kg destrio", destrio)
        setc("usuario", usuario)
        setc("linea",   usuario)
        # If no matching columns, just append at end
        if all(v == "" for v in row):
            row = [ts, lote, destrio, usuario]

        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Confecciones!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row[:max(len(headers), 4)]]}
        ).execute()

        return jsonify({"ok": True, "lote": lote, "destrio": destrio})

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
            range=f"{MANIPULADO_SHEET}!A:O"
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
            # Keep all non-finalizado items; keep finalizado only from last 7 days
            if estado == "finalizado":
                try:
                    from datetime import datetime as dt2
                    fd = dt2.strptime(fecha, "%d/%m/%Y") if fecha else None
                    if fd and (dt2.now() - fd).days > 7:
                        continue
                except:
                    pass
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
                "lote": row[14] if len(row) > 14 else "",
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
        # Use fecha_salida from pedido if provided, else today
        fecha = str(data.get("fecha_salida", "")).strip() or datetime.now().strftime("%d/%m/%Y")
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
            str(data.get("observaciones", "")),
            str(data.get("lote", ""))
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
        if data.get("palet"):
            updates.append({"range": f"{MANIPULADO_SHEET}!F{row_num}", "values": [[str(data["palet"])]]})
        if data.get("lote"):
            updates.append({"range": f"{MANIPULADO_SHEET}!O{row_num}", "values": [[str(data["lote"])]]})
        if data.get("personas"):
            updates.append({"range": f"{MANIPULADO_SHEET}!M{row_num}", "values": [[str(data["personas"])]]})
        if estado == "finalizado":
            updates.append({"range": f"{MANIPULADO_SHEET}!J{row_num}", "values": [[data.get("inicio","")]]})
            updates.append({"range": f"{MANIPULADO_SHEET}!K{row_num}", "values": [[data.get("fin","")]]})
            updates.append({"range": f"{MANIPULADO_SHEET}!L{row_num}", "values": [[str(data.get("minutos",""))]]})
            updates.append({"range": f"{MANIPULADO_SHEET}!M{row_num}", "values": [[str(data.get("personas",""))]]})
            if data.get("lote"):
                updates.append({"range": f"{MANIPULADO_SHEET}!O{row_num}", "values": [[str(data.get("lote",""))]]})
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
