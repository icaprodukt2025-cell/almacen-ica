# 🏭 ASISTENTE DE ALMACÉN ICA PRODUKT
## Guía de instalación y puesta en marcha

---

## ¿QUÉ ES ESTO?
Una app web que tu jefe puede abrir desde el móvil y preguntar en lenguaje natural sobre los pedidos y cargas del día. Los datos se leen automáticamente de Google Drive.

---

## PASO 1 — PREPARAR EL ARCHIVO DE CREDENCIALES

1. Crea un archivo llamado `service_account.json` dentro de la carpeta del proyecto
2. Pega dentro el contenido del JSON que descargaste de Google Cloud
3. El archivo debe quedar así (ya lo tienes de antes):
   ```json
   {
     "type": "service_account",
     "project_id": "n8n-ica-487816",
     ...
   }
   ```

---

## PASO 2 — SUBIR EL CÓDIGO A GITHUB (GRATIS)

1. Crea una cuenta en **github.com** si no tienes
2. Crea un repositorio nuevo llamado `almacen-ica` (privado)
3. Sube todos los archivos de esta carpeta:
   - app.py
   - requirements.txt
   - Procfile
   - .gitignore
   - static/index.html
   - service_account.json  ← MUY IMPORTANTE (no lo publiques en público)

Para subir los archivos, puedes usar la opción "Upload files" directamente en GitHub desde el navegador.

---

## PASO 3 — DESPLEGAR EN RAILWAY (GRATIS)

Railway es un servicio gratuito que pone tu app en internet.

1. Entra en **railway.app** y regístrate con tu cuenta de Google
2. Clic en **"New Project"** → **"Deploy from GitHub repo"**
3. Selecciona el repositorio `almacen-ica`
4. Railway detectará automáticamente que es Python y lo configurará
5. En unos minutos tendrás una URL tipo: `https://almacen-ica-xxx.railway.app`

¡Esa URL es la que le das a tu jefe para que la abra en el móvil!

---

## USO DIARIO

Cada mañana, alguien del equipo tiene que:

1. Exportar el cuadre de pedidos → guardarlo como **pedidos.xlsx**
2. Exportar/guardar la hoja de cargas → guardarla como **cargas.xlsx**
3. Subir ambos archivos a la carpeta de Google Drive **"pedidos-almacen"**
   - URL directa: https://drive.google.com/drive/folders/1o3fD5O3N65DQjXlzjmo_NfMAgCzTvIDv
   - Simplemente arrastrar y soltar, reemplazando los anteriores

¡Y listo! Tu jefe abre la app y pregunta lo que necesite.

---

## EJEMPLOS DE PREGUNTAS

- "¿Qué pedidos tenemos hoy?"
- "El pedido 914, ¿qué lleva y quién lo recoge?"
- "¿Hay alguna incidencia o pedido anulado?"
- "¿Qué matrícula lleva el pedido 915?"
- "¿Cuántos bultos tiene el pedido de TEGUT?"
- "¿Qué transportistas salen hoy?"
- "Dame un resumen de todos los pedidos de ALDI"

---

## SOLUCIÓN DE PROBLEMAS

**"Sin datos" en la app:**
→ Verifica que los archivos en Drive se llaman exactamente `pedidos.xlsx` y `cargas.xlsx`
→ Verifica que la carpeta está compartida con `almacen-bot@n8n-ica-487816.iam.gserviceaccount.com`

**La app no responde:**
→ Verifica en railway.app que el servicio está activo (plan gratuito tiene límites de horas)

---

## DATOS DE CONFIGURACIÓN

- **Google Drive Folder ID:** 1o3fD5O3N65DQjXlzjmo_NfMAgCzTvIDv
- **Cuenta de servicio:** almacen-bot@n8n-ica-487816.iam.gserviceaccount.com
- **IA:** Google Gemini (gratuito)
