"""
Bot WhatsApp Cosecha - Prototipo Funcional
Responde consultas de SQL Server vía WhatsApp Business
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import pyodbc
import requests
import os
from dotenv import load_dotenv
import logging
import anthropic

load_dotenv()

app = FastAPI()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

DB_HOST = os.getenv("DB_HOST", "192.168.200.9")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_USER = os.getenv("DB_USER", "us_consultas")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "Control_EAS")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID")
WHATSAPP_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "token_seguro_12345")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ============================================================================
# CONEXIÓN SQL SERVER
# ============================================================================

def conectar_sql():
    """Conecta a SQL Server"""
    try:
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={DB_HOST},{DB_PORT};"
            f"Database={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )
        conn = pyodbc.connect(conn_str, timeout=5)
        return conn
    except Exception as e:
        logger.error(f"Error conexión SQL: {str(e)}")
        return None

# ============================================================================
# CONSULTAS A SQL SERVER
# ============================================================================

def obtener_bins_estimados(variedad):
    """Consulta: ¿Cuántos bins estimados de [variedad]?"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"
        
        cursor = conn.cursor()
        query = """
        SELECT SUM(Bultos) as total_bins
        FROM [PKG-Cos-Estima]
        WHERE [Variedad Agronomica] LIKE ?
        """
        cursor.execute(query, (f"%{variedad}%",))
        resultado = cursor.fetchone()
        conn.close()
        
        if resultado and resultado[0]:
            return f"📦 {variedad.upper()}: {int(resultado[0])} bins estimados esta temporada"
        else:
            return f"No hay datos de {variedad} en base de datos"
    except Exception as e:
        logger.error(f"Error en obtener_bins_estimados: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_cosecha_actual(variedad):
    """Consulta: ¿Cuántos bins se cosecharon hoy de [variedad]?"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"
        
        cursor = conn.cursor()
        query = """
        SELECT SUM(Bultos) as total
        FROM [Recepcion_Consolidada]
        WHERE Variedad LIKE ? AND CAST(Fecha AS DATE) = CAST(GETDATE() AS DATE)
        """
        cursor.execute(query, (f"%{variedad}%",))
        resultado = cursor.fetchone()
        conn.close()
        
        if resultado and resultado[0]:
            return f"✅ {variedad.upper()}: {int(resultado[0])} bins cosechados hoy"
        else:
            return f"Sin cosecha de {variedad} hoy"
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_actual: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_calibre_promedio(variedad, ano=2025):
    """Consulta: ¿Cuál fue el calibre promedio de [variedad] el año pasado?"""
    return f"📏 Consulta de calibre para {variedad.upper()} aún no disponible: falta confirmar con Erick en qué tabla vive el dato de calibre."

# ============================================================================
# PROCESAMIENTO DE MENSAJES
# ============================================================================

# Alias de variedad tal como la escribe el usuario -> nombre real en Control_EAS
# "tifany" es el nombre real en base de datos (con una sola "f")
ALIAS_VARIEDADES = {
    "tiffany": "tifany",
}

def normalizar_variedad(variedad_usuario):
    """Traduce variedades con alias conocidos (ej. 'tiffany' -> 'tifany') al nombre real en la BD"""
    v = variedad_usuario.lower().strip()
    return ALIAS_VARIEDADES.get(v, v)

TOOLS = [
    {
        "name": "consultar_bins_estimados",
        "description": "Consulta cuántos bins se estiman cosechar esta temporada para una variedad de fruta.",
        "input_schema": {
            "type": "object",
            "properties": {
                "variedad": {
                    "type": "string",
                    "description": "Nombre de la variedad mencionada por el usuario, tal como la escribió (ej. 'tiffany', 'crimson')."
                }
            },
            "required": ["variedad"]
        }
    },
    {
        "name": "consultar_cosecha_hoy",
        "description": "Consulta cuántos bins se han cosechado HOY para una variedad de fruta.",
        "input_schema": {
            "type": "object",
            "properties": {
                "variedad": {
                    "type": "string",
                    "description": "Nombre de la variedad mencionada por el usuario."
                }
            },
            "required": ["variedad"]
        }
    },
    {
        "name": "consultar_calibre_promedio",
        "description": "Consulta el calibre promedio histórico de una variedad de fruta.",
        "input_schema": {
            "type": "object",
            "properties": {
                "variedad": {
                    "type": "string",
                    "description": "Nombre de la variedad mencionada por el usuario."
                }
            },
            "required": ["variedad"]
        }
    },
]

SYSTEM_PROMPT = """Eres el asistente de WhatsApp de Agua Santa para consultas de cosecha de fruta.

Tienes herramientas para consultar bins estimados, cosecha de hoy, y calibre promedio por variedad.
Usa una herramienta cuando el usuario pregunte por alguno de esos datos y haya mencionado (o puedas inferir)
una variedad de fruta.

Si el usuario saluda, pide ayuda, o pregunta algo que no corresponde a ninguna herramienta, respóndele tú
directamente: breve, amable, en español, y si corresponde explícale qué puedes hacer (bins estimados,
cosecha de hoy, calibre promedio, por variedad).

Si falta la variedad para poder consultar, pídesela al usuario en vez de inventar una."""

def ejecutar_tool(tool_name, tool_input):
    variedad = normalizar_variedad(tool_input.get("variedad", ""))
    if tool_name == "consultar_bins_estimados":
        return obtener_bins_estimados(variedad)
    elif tool_name == "consultar_cosecha_hoy":
        return obtener_cosecha_actual(variedad)
    elif tool_name == "consultar_calibre_promedio":
        return obtener_calibre_promedio(variedad)
    return "No supe qué información buscar para esa pregunta."

def procesar_mensaje(texto_mensaje):
    """
    Usa Claude para interpretar el mensaje: decide si llamar una herramienta de consulta
    o responder directamente (saludo, ayuda, pregunta fuera de alcance).
    """
    try:
        response = claude_client.messages.create(
            model="claude-sonnet-5",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=[{"role": "user", "content": texto_mensaje}],
        )

        tool_use_block = next((b for b in response.content if b.type == "tool_use"), None)
        if tool_use_block:
            return ejecutar_tool(tool_use_block.name, tool_use_block.input)

        text_block = next((b for b in response.content if b.type == "text"), None)
        if text_block:
            return text_block.text

        return "No entendí tu pregunta. Escribe 'ayuda' para ver qué puedo hacer."
    except Exception as e:
        logger.error(f"Error en procesar_mensaje (Claude): {str(e)}")
        return "Tuve un problema procesando tu mensaje. Intenta de nuevo en un momento."

# ============================================================================
# ENVÍO POR WHATSAPP
# ============================================================================

def enviar_whatsapp(numero_destino, mensaje_texto):
    """
    Envía mensaje por WhatsApp Business API (Meta)
    """
    try:
        url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/messages"
        
        headers = {
            "Authorization": f"Bearer {WHATSAPP_TOKEN}",
            "Content-Type": "application/json",
        }
        
        payload = {
            "messaging_product": "whatsapp",
            "to": numero_destino,
            "type": "text",
            "text": {"body": mensaje_texto}
        }
        
        response = requests.post(url, json=payload, headers=headers)
        logger.info(f"WhatsApp enviado a {numero_destino}: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error enviando WhatsApp: {str(e)}")
        return False

# ============================================================================
# WEBHOOKS FASTAPI
# ============================================================================

@app.get("/webhook")
async def verify_webhook(request: Request):
    """
    Verifica webhook con Meta (GET request)
    """
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    
    if mode and token:
        if mode == "subscribe" and token == WHATSAPP_VERIFY_TOKEN:
            logger.info("Webhook verificado correctamente")
            return int(challenge)
        else:
            logger.warning("Token de verificación inválido")
            return JSONResponse({"status": "error"}, status_code=403)
    
    return JSONResponse({"status": "error"}, status_code=400)

@app.post("/webhook")
async def receive_message(request: Request):
    """
    Recibe mensajes de WhatsApp (POST request)
    """
    try:
        data = await request.json()
        logger.info(f"Webhook recibido: {data}")
        
        # Extraer información del mensaje
        if data.get("entry"):
            for entry in data["entry"]:
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    messages = value.get("messages", [])
                    
                    for message in messages:
                        numero_sender = message.get("from")
                        msg_id = message.get("id")
                        msg_text = message.get("text", {}).get("body", "")
                        
                        logger.info(f"Mensaje de {numero_sender}: {msg_text}")
                        
                        # Procesar mensaje
                        respuesta = procesar_mensaje(msg_text)
                        
                        # Enviar respuesta
                        enviar_whatsapp(numero_sender, respuesta)
        
        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Error en webhook: {str(e)}")
        return JSONResponse({"status": "error"}, status_code=500)

# ============================================================================
# RUTAS DE PRUEBA (LOCAL)
# ============================================================================

from fastapi.responses import HTMLResponse

@app.get("/privacy", response_class=HTMLResponse)
async def privacy():
    return """
    <html><head><meta charset="utf-8"><title>Política de Privacidad</title></head>
    <body style="font-family:sans-serif;max-width:700px;margin:40px auto;line-height:1.6">
    <h1>Política de Privacidad — Bot Cosecha Agua Santa</h1>
    <p>Última actualización: julio 2026</p>

    <h2>Alcance</h2>
    <p>Esta aplicación es una herramienta interna de Empresas Agua Santa.
    Su uso está restringido a personal autorizado y productores asociados.</p>

    <h2>Datos que se procesan</h2>
    <p>Número de teléfono de WhatsApp y el contenido de los mensajes enviados
    al servicio, con el único fin de responder consultas sobre programación
    y avance de cosecha.</p>

    <h2>Uso de la información</h2>
    <p>Los datos se utilizan exclusivamente para operar el servicio de consultas.
    No se venden, ceden ni comparten con terceros, ni se usan con fines publicitarios.</p>

    <h2>Conservación</h2>
    <p>Los mensajes se procesan de forma transitoria. Los registros operativos
    se conservan solo el tiempo necesario para el funcionamiento del sistema.</p>

    <h2>Contacto</h2>
    <p>Consultas sobre esta política: elazo@aguasanta.cl</p>
    </body></html>
    """

@app.get("/health")
async def health():
    """Verifica estado del bot"""
    return {"status": "Bot activo y listo"}

@app.get("/test/mensaje")
async def test_mensaje(mensaje: str):
    """
    TEST LOCAL: Envía un mensaje y obtiene respuesta
    Usa: curl -X POST "http://localhost:8000/test/mensaje?mensaje=cuantos%20bins%20de%20tiffany"
    """
    respuesta = procesar_mensaje(mensaje)
    return {"pregunta": mensaje, "respuesta": respuesta}

@app.get("/test/conexion")
async def test_conexion():
    """TEST LOCAL: Verifica conexión a SQL Server"""
    conn = conectar_sql()
    if conn:
        conn.close()
        return {"status": "Conexión SQL OK"}
    else:
        return {"status": "Error conexión SQL", "error": "Revisa credenciales"}

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
