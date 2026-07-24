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
        # Usa la tabla que Erick proporcionó
        query = """
        SELECT SUM(Cantidad) as total_bins
        FROM [Tabla PKG-Cos-Estima]
        WHERE Variedad LIKE ?
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
        SELECT SUM(Cantidad) as total
        FROM [vista Recepcion_Consolidada]
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
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"
        
        cursor = conn.cursor()
        query = """
        SELECT AVG(Calibre) as calibre_prom
        FROM [tabla_calibres]
        WHERE Variedad LIKE ? AND YEAR(Fecha) = ?
        """
        cursor.execute(query, (f"%{variedad}%", ano))
        resultado = cursor.fetchone()
        conn.close()
        
        if resultado and resultado[0]:
            return f"📏 {variedad.upper()}: Calibre promedio {ano}: {resultado[0]:.1f}"
        else:
            return f"No hay datos de calibre para {variedad} en {ano}"
    except Exception as e:
        logger.error(f"Error en obtener_calibre_promedio: {str(e)}")
        return f"Error al consultar: {str(e)}"

# ============================================================================
# PROCESAMIENTO DE MENSAJES
# ============================================================================

def procesar_mensaje(texto_mensaje):
    """
    Procesa el mensaje y devuelve respuesta inteligente
    """
    texto = texto_mensaje.lower().strip()
    
    # Palabras clave y respuestas
    if "bins estimados" in texto or "cuántos bins" in texto:
        # Buscar variedad mencionada
        for variedad in ["tiffany", "crimson", "flame", "jumbo red", "thompson"]:
            if variedad in texto:
                return obtener_bins_estimados(variedad)
        return "Por favor menciona una variedad (Tiffany, Crimson, Flame, etc)"
    
    elif "cosechado" in texto or "cosecha de hoy" in texto:
        for variedad in ["tiffany", "crimson", "flame", "jumbo red", "thompson"]:
            if variedad in texto:
                return obtener_cosecha_actual(variedad)
        return "¿Cuál variedad? (Tiffany, Crimson, Flame, etc)"
    
    elif "calibre" in texto or "promedio" in texto:
        for variedad in ["tiffany", "crimson", "flame", "jumbo red", "thompson"]:
            if variedad in texto:
                return obtener_calibre_promedio(variedad)
        return "¿De cuál variedad quieres conocer el calibre?"
    
    elif "ayuda" in texto or "hola" in texto or "?" in texto:
        return """🤖 BOT COSECHA - Qué puedo hacer:
        
📦 "¿Cuántos bins de Tiffany?" → Bins estimados
✅ "Cosecha de Crimson" → Bins cosechados hoy
📏 "Calibre promedio Flame" → Datos históricos

Variedad disponibles: Tiffany, Crimson, Flame, Thompson, Jumbo Red"""
    
    else:
        return "No entendí tu pregunta. Escribe 'ayuda' para ver qué puedo hacer."

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
