"""
Bot WhatsApp Cosecha - Prototipo Funcional
Responde consultas de SQL Server vía WhatsApp Business
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import pyodbc
import requests
import os
import io
import sqlite3
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import logging
import anthropic
import openai
from apscheduler.schedulers.background import BackgroundScheduler

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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

HISTORIAL_CLAVE = os.getenv("HISTORIAL_CLAVE", "cambiar_esta_clave")
ADMIN_CLAVE = os.getenv("ADMIN_CLAVE", "cambiar_esta_clave")

# --- Envíos automáticos (Fase 2: cuestionarios de proyección y alertas de desviación) ---
# Mientras ENVIOS_AUTOMATICOS_PRODUCCION no sea "1", TODOS los envíos automáticos van solo
# a los números con rol admin (modo prueba), sin importar a qué rol estaban dirigidos.
ENVIOS_AUTOMATICOS_PRODUCCION = os.getenv("ENVIOS_AUTOMATICOS_PRODUCCION", "0") == "1"
CUESTIONARIO_HORA_AM = os.getenv("CUESTIONARIO_HORA_AM", "08:00")
CUESTIONARIO_HORA_PM = os.getenv("CUESTIONARIO_HORA_PM", "15:00")
# Resumen semanal informativo de precosecha (lunes), separado de la alerta de desviaciones.
RESUMEN_SEMANAL_DIA = os.getenv("RESUMEN_SEMANAL_DIA", "mon")
RESUMEN_SEMANAL_HORA = os.getenv("RESUMEN_SEMANAL_HORA", "08:00")
ALERTA_SEMANAL_DIA = os.getenv("ALERTA_SEMANAL_DIA", "mon")  # día en formato cron: mon, tue, ...
ALERTA_SEMANAL_HORA = os.getenv("ALERTA_SEMANAL_HORA", "08:00")
UMBRAL_DESVIACION_PCT = float(os.getenv("UMBRAL_DESVIACION_PCT", "15"))
# Variedades con estimado acumulado bajo este mínimo no generan alerta (evita ruido de
# variedades chicas donde un % de desviación grande son pocos kilos).
ALERTA_MIN_KG = float(os.getenv("ALERTA_MIN_KG", "1000"))
# La tabla de la alerta solo lista variedades CON movimiento (estimado o real) en los últimos
# N días: son las accionables. Las desviadas sin movimiento (cosecha terminada o no iniciada)
# se resumen en una línea aparte. 0 = sin filtro (verificado: sin esto salen 56 de 82
# variedades, la mayoría ya cerradas). Configurable con ALERTA_DIAS_ACTIVIDAD.
ALERTA_DIAS_ACTIVIDAD = int(os.getenv("ALERTA_DIAS_ACTIVIDAD", "14"))
# Máximo de filas en la tabla de la alerta (ordenada por diferencia en kg, así lo grande
# queda arriba aunque su % sea menor); el resto se resume en una línea.
ALERTA_MAX_FILAS = int(os.getenv("ALERTA_MAX_FILAS", "30"))

# ============================================================================
# HISTORIAL LOCAL DE CONVERSACIONES (SQLite, no toca el SQL Server de Agua Santa)
# ============================================================================

DB_LOCAL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conversaciones.db")

def inicializar_db_local():
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_sender TEXT,
            tipo_mensaje TEXT,
            mensaje TEXT,
            respuesta TEXT,
            fecha_hora TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS numeros_permitidos (
            numero TEXT PRIMARY KEY,
            nombre TEXT,
            fecha_agregado TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cuestionarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero TEXT,
            turno TEXT,
            mensaje TEXT,
            estado TEXT DEFAULT 'pendiente',
            respuesta TEXT,
            ajuste TEXT,
            fecha_hora_envio TEXT DEFAULT (datetime('now', 'localtime')),
            fecha_hora_respuesta TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alertas_enviadas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT,
            contenido TEXT,
            destinatarios TEXT,
            fecha_hora TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    # Estado real de entrega de cada mensaje que envía el bot. La API de Meta responde 200 y
    # un id apenas acepta el mensaje; si después NO se entrega, eso solo llega por los
    # "statuses" del webhook. Sin esta tabla, un mensaje que nunca llegó se veía como enviado.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mensajes_estado (
            wamid TEXT PRIMARY KEY,
            numero TEXT,
            estado TEXT,
            error_code TEXT,
            error_detalle TEXT,
            fecha_envio TEXT DEFAULT (datetime('now', 'localtime')),
            fecha_estado TEXT
        )
    """)
    # Fundos (productores) que cada número tiene asignados. Zonal y productor solo pueden
    # consultar y recibir información de los suyos; admin, gerencia y EAS ven todo.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS numeros_fundos (
            numero TEXT,
            fundo TEXT,
            PRIMARY KEY (numero, fundo)
        )
    """)
    columnas = [fila[1] for fila in conn.execute("PRAGMA table_info(numeros_permitidos)")]
    if "rol" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN rol TEXT DEFAULT 'productor'")
    if "productor" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN productor TEXT")
    # Gerencia y EAS solo reciben envíos automáticos si se les activa explícitamente
    # ("reciben notificaciones sólo si lo necesitan").
    if "recibe_notificaciones" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN recibe_notificaciones INTEGER DEFAULT 0")
    # Migración: el productor único que había antes pasa a ser el primer fundo asignado.
    conn.execute("""
        INSERT OR IGNORE INTO numeros_fundos (numero, fundo)
        SELECT numero, productor FROM numeros_permitidos
        WHERE productor IS NOT NULL AND TRIM(productor) <> ''
    """)
    conn.commit()
    conn.close()

inicializar_db_local()

# ============================================================================
# CONTROL DE ACCESO: solo los números en numeros_permitidos reciben respuesta
# ============================================================================

def normalizar_numero(numero):
    """Deja solo dígitos (WhatsApp manda el 'from' sin '+', pero por si lo pegan con
    espacios, guiones o el '+' delante al administrar la lista)."""
    return "".join(c for c in (numero or "") if c.isdigit())

def numero_esta_permitido(numero):
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        cursor = conn.execute(
            "SELECT 1 FROM numeros_permitidos WHERE numero = ?",
            (normalizar_numero(numero),)
        )
        existe = cursor.fetchone() is not None
        conn.close()
        return existe
    except Exception as e:
        logger.error(f"Error verificando número permitido: {str(e)}")
        return False

# Perfiles:
#   admin     - solo Edson: ve y hace todo, y recibe siempre los avisos del EAS.
#   gerencia  - consulta toda la información; recibe avisos solo si se le activa.
#   eas       - consulta toda la información de todos los productores; avisos solo si se activa.
#   zonal     - consulta solo SUS fundos; recibe avisos de ajuste y el resumen semanal.
#   productor - consulta solo SUS fundos, responde cuestionarios y reporta correcciones.
ROLES_VALIDOS = ("admin", "gerencia", "eas", "zonal", "productor")
# Roles sin restricción de fundos (ven la información de todos los productores).
ROLES_VEN_TODO = ("admin", "gerencia", "eas")
# Roles cuyo acceso se limita a los fundos que tengan asignados.
ROLES_ACOTADOS = ("zonal", "productor")
ROL_POR_DEFECTO = "productor"

def agregar_numero_permitido(numero, nombre=None, rol=None, productor=None, recibe_notificaciones=None):
    """Inserta o actualiza un número. Los campos que vengan en None no se tocan, para no
    pisar lo existente al re-agregar un número solo para cambiarle el nombre. `productor`
    se agrega como un fundo más (un número puede tener varios)."""
    conn = sqlite3.connect(DB_LOCAL_PATH)
    num = normalizar_numero(numero)
    existe = conn.execute("SELECT 1 FROM numeros_permitidos WHERE numero = ?", (num,)).fetchone()
    if existe:
        conn.execute(
            "UPDATE numeros_permitidos SET nombre = COALESCE(?, nombre), rol = COALESCE(?, rol), "
            "recibe_notificaciones = COALESCE(?, recibe_notificaciones) WHERE numero = ?",
            (nombre, rol, recibe_notificaciones, num)
        )
    else:
        conn.execute(
            "INSERT INTO numeros_permitidos (numero, nombre, rol, recibe_notificaciones) "
            "VALUES (?, ?, ?, ?)",
            (num, nombre, rol or ROL_POR_DEFECTO, recibe_notificaciones or 0)
        )
    conn.commit()
    conn.close()
    if productor:
        asignar_fundos(num, [productor])

def rol_de(numero):
    """Rol del número, o None si no tiene acceso."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT rol FROM numeros_permitidos WHERE numero = ?", (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return (fila[0] or ROL_POR_DEFECTO) if fila else None
    except Exception as e:
        logger.error(f"Error obteniendo rol: {str(e)}")
        return None

def fundos_de(numero):
    """Fundos (productores) asignados a ese número."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        filas = conn.execute(
            "SELECT fundo FROM numeros_fundos WHERE numero = ? ORDER BY fundo",
            (normalizar_numero(numero),)
        ).fetchall()
        conn.close()
        return [f[0] for f in filas]
    except Exception as e:
        logger.error(f"Error obteniendo fundos: {str(e)}")
        return []

def asignar_fundos(numero, fundos, reemplazar=False):
    """Asigna fundos a un número. Con reemplazar=True deja exactamente los indicados."""
    num = normalizar_numero(numero)
    conn = sqlite3.connect(DB_LOCAL_PATH)
    if reemplazar:
        conn.execute("DELETE FROM numeros_fundos WHERE numero = ?", (num,))
    for fundo in fundos:
        fundo = (fundo or "").strip()
        if fundo:
            conn.execute(
                "INSERT OR IGNORE INTO numeros_fundos (numero, fundo) VALUES (?, ?)", (num, fundo)
            )
    conn.commit()
    conn.close()

def quitar_fundo(numero, fundo):
    conn = sqlite3.connect(DB_LOCAL_PATH)
    cursor = conn.execute(
        "DELETE FROM numeros_fundos WHERE numero = ? AND fundo = ?",
        (normalizar_numero(numero), (fundo or "").strip())
    )
    eliminado = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return eliminado

def alcance_de(numero):
    """Qué fundos puede consultar ese número:
       None      -> sin restricción (admin, gerencia, eas)
       []        -> rol acotado SIN fundos asignados: no debe ver nada
       [fundos]  -> rol acotado, solo esos fundos
    """
    rol = rol_de(numero)
    if rol is None or rol in ROLES_VEN_TODO:
        return None
    return fundos_de(numero)

def _sql_alcance(alcance):
    """Condición SQL que limita una consulta a los fundos permitidos, como (sql, params).
    Es la barrera que impide que un zonal o productor vea fundos que no son suyos."""
    if alcance is None:
        return None, []
    if not alcance:
        # Rol acotado sin fundos asignados: no puede ver ninguna fila.
        return "1 = 0", []
    ors = " OR ".join(["Productor LIKE ?"] * len(alcance))
    return f"({ors})", [f"%{f}%" for f in alcance]

MENSAJE_SIN_FUNDOS = (
    "Todavía no tienes fundos asignados, así que no puedo mostrarte información. "
    "Pídele a quien administra el asistente que te asigne tus fundos."
)

def numeros_por_rol(roles, solo_con_notificaciones=False):
    """Números permitidos cuyo rol está en `roles`, con sus fundos asignados.
    Con solo_con_notificaciones=True descarta los que tengan el aviso desactivado
    (gerencia y EAS parten apagados: reciben 'solo si lo necesitan')."""
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.row_factory = sqlite3.Row
    marcadores = ",".join("?" for _ in roles)
    cursor = conn.execute(
        f"SELECT numero, nombre, rol, recibe_notificaciones FROM numeros_permitidos "
        f"WHERE rol IN ({marcadores})",
        list(roles)
    )
    filas = [dict(row) for row in cursor.fetchall()]
    conn.close()
    resultado = []
    for fila in filas:
        rol = fila["rol"] or ROL_POR_DEFECTO
        if solo_con_notificaciones and rol in ("gerencia", "eas") and not fila["recibe_notificaciones"]:
            continue
        fila["fundos"] = fundos_de(fila["numero"])
        resultado.append(fila)
    return resultado

def quitar_numero_permitido(numero):
    conn = sqlite3.connect(DB_LOCAL_PATH)
    cursor = conn.execute("DELETE FROM numeros_permitidos WHERE numero = ?", (normalizar_numero(numero),))
    eliminado = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return eliminado

def listar_numeros_permitidos():
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT numero, nombre, rol, recibe_notificaciones, fecha_agregado "
        "FROM numeros_permitidos ORDER BY fecha_agregado DESC"
    )
    filas = [dict(row) for row in cursor.fetchall()]
    for fila in filas:
        fila["fundos"] = fundos_de(fila["numero"])
        fila["ve_todo"] = (fila["rol"] or ROL_POR_DEFECTO) in ROLES_VEN_TODO
    conn.close()
    return filas

def guardar_conversacion(numero_sender, tipo_mensaje, mensaje, respuesta):
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute(
            "INSERT INTO conversaciones (numero_sender, tipo_mensaje, mensaje, respuesta) VALUES (?, ?, ?, ?)",
            (numero_sender, tipo_mensaje, mensaje, respuesta)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error guardando conversación: {str(e)}")

LIMITE_HISTORIAL_MENSAJES = 6
LIMITE_HISTORIAL_MINUTOS = 60

def obtener_historial_conversacion(numero_sender):
    """
    Trae los últimos mensajes recientes de ESE número (misma sesión de chat) para darle
    contexto a Claude y que pueda entender preguntas de seguimiento (ej. "y de Lapins?",
    "conviértelo a bins"). Solo mensajes de la última hora, para no arrastrar contexto viejo.
    """
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """SELECT mensaje, respuesta FROM conversaciones
               WHERE numero_sender = ?
               AND fecha_hora >= datetime('now', 'localtime', ?)
               ORDER BY id DESC LIMIT ?""",
            (numero_sender, f"-{LIMITE_HISTORIAL_MINUTOS} minutes", LIMITE_HISTORIAL_MENSAJES)
        )
        filas = list(cursor.fetchall())
        conn.close()
        filas.reverse()
        return filas
    except Exception as e:
        logger.error(f"Error obteniendo historial de conversación: {str(e)}")
        return []

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

# Tabla oficial: Recepcion_Consolidada, distinguida por [Base Origen]
# 'Recepción Planta' = cosecha real. Hay 3 fuentes de estimado: 'Trisemanal' (rolling 3 semanas),
# 'Estim Invierno' y 'Estim Primavera' (dos ciclos de planificación de temporada).
# Por defecto, "estimado" usa Estim Primavera (pedido explícito del usuario: Primavera es la
# referencia habitual; Trisemanal/Invierno solo se usan si se piden por su nombre).
BASE_ORIGEN_REAL = "Recepción Planta"
BASE_ORIGEN_TRISEMANAL = "Trisemanal"
BASE_ORIGEN_ESTIM_INVIERNO = "Estim Invierno"
BASE_ORIGEN_ESTIM_PRIMAVERA = "Estim Primavera"
BASE_ORIGEN_ESTIMADO = BASE_ORIGEN_ESTIM_PRIMAVERA
# Fuente de los cuestionarios diarios y del resumen semanal: la trisemanal es la proyección
# operativa de corto plazo (la que dice qué se cosecha estos días), a diferencia de Estim
# Primavera que cubre la temporada entera y es la que usan las consultas del bot.
BASE_ORIGEN_CUESTIONARIO = BASE_ORIGEN_TRISEMANAL

FUENTES_ESTIMADO = {
    "primavera": BASE_ORIGEN_ESTIM_PRIMAVERA,
    "trisemanal": BASE_ORIGEN_TRISEMANAL,
    "invierno": BASE_ORIGEN_ESTIM_INVIERNO,
}

def resolver_fuente_estimado(fuente_estimado):
    """Devuelve el valor real de [Base Origen] a usar como 'estimado'. Por defecto (None u
    omitido) es Estim Primavera; solo cambia si se pide explícitamente trisemanal/invierno."""
    return FUENTES_ESTIMADO.get((fuente_estimado or "").strip().lower(), BASE_ORIGEN_ESTIMADO)

# Etiquetas de visualización pedidas por el negocio para las respuestas del bot (no cambian
# el valor real de [Base Origen] usado en las consultas, solo cómo se muestra al usuario).
ETIQUETA_FUENTE = {
    BASE_ORIGEN_TRISEMANAL: "Trisemanal",
    BASE_ORIGEN_ESTIM_INVIERNO: "Estim. Inv",
    BASE_ORIGEN_ESTIM_PRIMAVERA: "Precosecha",
}

def rango_temporada(temporada):
    """
    Rango exacto de fechas de una temporada, según la definición del negocio: la temporada N
    va desde el lunes de la semana ISO 44 del año N-1 hasta el domingo de la semana ISO 43 del
    año N (el día antes de que empiece la temporada N+1).
    Ej.: temporada 2026 = 2025-10-27 a 2026-10-25; temporada 2025 = 2024-10-28 a 2025-10-26.
    No depende de qué datos existan en la base, para no dar un rango incompleto si faltan
    registros cerca de los bordes de la temporada.
    """
    inicio = date.fromisocalendar(temporada - 1, 44, 1)
    fin = date.fromisocalendar(temporada, 44, 1) - timedelta(days=1)
    return inicio, fin

def etiqueta_semana(numero_semana):
    """Número de semana con 2 dígitos (columna Semana de la base: semana ISO estándar del
    año calendario, verificado contra Fecha: 2025-10-27 = semana 44)."""
    return f"{int(numero_semana):02d}"

def orden_semana(numero_semana):
    """Clave de orden para que las semanas de una temporada queden en su orden natural
    (44, 45, ..., 52, 1, 2, ..., 43), no en orden numérico simple: una temporada abarca
    las semanas 44-52 del año anterior y 1-43 del año de la temporada."""
    numero_semana = int(numero_semana)
    return numero_semana if numero_semana >= 44 else numero_semana + 100

ESPECIE_TRADUCCION = {
    "GRAPE": "Uva",
    "MANDARIN": "Mandarina",
    "CHERRY": "Cereza",
    "PEACH": "Durazno",
    "NECTARINE": "Nectarín",
    "PLUM": "Ciruela",
    "PEAR": "Pera",
    "APRICOT": "Damasco",
    "BLUEBERRY": "Arándano",
    "KIWI": "Kiwi",
    "ORANGE": "Naranja",
    "FLAT PEACH": "Durazno Plano",
}

def traducir_especie(especie):
    if not especie:
        return especie
    return ESPECIE_TRADUCCION.get(especie.upper().strip(), especie)

def formatear_kg(valor):
    """Formatea un entero con separador de miles al estilo chileno (punto)"""
    return f"{int(valor):,}".replace(",", ".")

def obtener_bins_estimados(variedad, fuente_estimado=None, temporada=None, alcance=None):
    """Consulta: ¿Cuántos kg se estiman cosechar de [variedad] en una temporada? Por defecto usa
    la temporada vigente y Estim Primavera; fuente_estimado puede forzar 'trisemanal' o
    'invierno' explícitamente, y temporada puede pedir una temporada distinta a la vigente
    (ej. la próxima, que puede tener Invierno/Primavera cargados antes de que empiece)."""
    try:
        base = resolver_fuente_estimado(fuente_estimado)
        temporada = temporada or TEMPORADA_ACTUAL
        if not temporada:
            return "No pude determinar la temporada vigente. ¿Puedes indicarme el número de temporada?"
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        sql_alc, params_alc = _sql_alcance(alcance)
        query = f"""
        SELECT SUM(KgsRecepcionados) as total
        FROM [Recepcion_Consolidada]
        WHERE Variedad LIKE ?
        AND [Base Origen] = ?
        AND Temporada = ?
        {"AND " + sql_alc if sql_alc else ""}
        """
        cursor.execute(query, [f"%{variedad}%", base, temporada] + params_alc)
        resultado = cursor.fetchone()
        conn.close()

        etiqueta = ETIQUETA_FUENTE.get(base, base)
        if resultado and resultado[0]:
            return f"📦 {variedad.upper()}: {formatear_kg(resultado[0])} kg estimados para la temporada {temporada} ({etiqueta})"
        else:
            return f"No hay estimación ({etiqueta}) registrada para {variedad} en la temporada {temporada}"
    except Exception as e:
        logger.error(f"Error en obtener_bins_estimados: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_cosecha_actual(variedad, fecha=None, alcance=None):
    """Consulta: ¿Cuántos kg se cosecharon (real) de [variedad] en una fecha? (default: hoy). fecha en formato YYYY-MM-DD"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        sql_alc, params_alc = _sql_alcance(alcance)
        query = f"""
        SELECT SUM(KgsRecepcionados) as total
        FROM [Recepcion_Consolidada]
        WHERE Variedad LIKE ?
        AND [Base Origen] = ?
        AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE))
        {"AND " + sql_alc if sql_alc else ""}
        """
        cursor.execute(query, [f"%{variedad}%", BASE_ORIGEN_REAL, fecha] + params_alc)
        resultado = cursor.fetchone()
        conn.close()

        etiqueta_fecha = fecha if fecha else "hoy"
        if resultado and resultado[0]:
            return f"✅ {variedad.upper()}: {formatear_kg(resultado[0])} kg cosechados ({etiqueta_fecha})"
        else:
            return f"Sin cosecha real registrada de {variedad} ({etiqueta_fecha})"
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_actual: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_calibre_promedio(variedad, ano=2025):
    """Consulta: ¿Cuál fue el calibre promedio de [variedad] el año pasado?"""
    return f"📏 Consulta de calibre para {variedad.upper()} aún no disponible: falta confirmar con Erick en qué tabla vive el dato de calibre."

def obtener_comparacion_estimado_vs_cosechado(variedad, fecha=None, fuente_estimado=None, alcance=None):
    """Consulta: ¿Cómo vamos de [variedad] respecto a lo estimado, en una fecha? (default: hoy).
    fecha en formato YYYY-MM-DD. Por defecto compara contra Estim Primavera; fuente_estimado
    puede forzar 'trisemanal' o 'invierno' explícitamente."""
    try:
        base = resolver_fuente_estimado(fuente_estimado)
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        sql_alc, params_alc = _sql_alcance(alcance)
        filtro_alc = f" AND {sql_alc}" if sql_alc else ""
        cursor.execute(
            f"""SELECT SUM(KgsRecepcionados) FROM [Recepcion_Consolidada]
               WHERE Variedad LIKE ? AND [Base Origen] = ?
               AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE)){filtro_alc}""",
            [f"%{variedad}%", base, fecha] + params_alc
        )
        fila = cursor.fetchone()
        estimado = fila[0] if fila and fila[0] else 0

        cursor.execute(
            f"""SELECT SUM(KgsRecepcionados) FROM [Recepcion_Consolidada]
               WHERE Variedad LIKE ? AND [Base Origen] = ?
               AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE)){filtro_alc}""",
            [f"%{variedad}%", BASE_ORIGEN_REAL, fecha] + params_alc
        )
        fila = cursor.fetchone()
        real = fila[0] if fila and fila[0] else 0

        conn.close()

        etiqueta_fecha = fecha if fecha else "hoy"

        if not estimado and not real:
            return f"No hay datos (ni estimado ni cosecha real) de {variedad} para {etiqueta_fecha}"

        etiqueta = ETIQUETA_FUENTE.get(base, base)
        if estimado and real:
            porcentaje = ((real - estimado) / estimado) * 100
            signo = "+" if porcentaje >= 0 else ""
            return (
                f"📊 {variedad.upper()} ({etiqueta_fecha}): {etiqueta} {formatear_kg(estimado)} kg, "
                f"real {formatear_kg(real)} kg ({signo}{porcentaje:.1f}% vs {etiqueta})"
            )
        elif estimado and not real:
            return f"📊 {variedad.upper()} ({etiqueta_fecha}): {etiqueta} {formatear_kg(estimado)} kg, aún sin cosecha real registrada"
        else:
            return f"📊 {variedad.upper()} ({etiqueta_fecha}): cosecha real {formatear_kg(real)} kg, no había {etiqueta} registrada para esa fecha"
    except Exception as e:
        logger.error(f"Error en obtener_comparacion_estimado_vs_cosechado: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_resumen_por_productor(productor, alcance=None):
    """Consulta: ¿Cuánto ha cosechado (real) el productor [productor] esta temporada?"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        sql_alc, params_alc = _sql_alcance(alcance)
        query = f"""
        SELECT SUM(KgsRecepcionados) as total, COUNT(DISTINCT Variedad) as variedades
        FROM [Recepcion_Consolidada]
        WHERE Productor LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
        {"AND " + sql_alc if sql_alc else ""}
        """
        cursor.execute(query, [f"%{productor}%", BASE_ORIGEN_REAL] + params_alc)
        resultado = cursor.fetchone()
        conn.close()

        if resultado and resultado[0]:
            return f"👨‍🌾 {productor.upper()}: {formatear_kg(resultado[0])} kg cosechados (real) en total, en {resultado[1]} variedades"
        else:
            return f"No encontré cosecha real registrada del productor '{productor}'"
    except Exception as e:
        logger.error(f"Error en obtener_resumen_por_productor: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_resumen_por_packing(packing, alcance=None):
    """Consulta: ¿Cuánto ha recibido (real) el packing [packing] esta temporada?"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        sql_alc, params_alc = _sql_alcance(alcance)
        query = f"""
        SELECT SUM(KgsRecepcionados) as total, COUNT(DISTINCT Variedad) as variedades
        FROM [Recepcion_Consolidada]
        WHERE Packing LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
        {"AND " + sql_alc if sql_alc else ""}
        """
        cursor.execute(query, [f"%{packing}%", BASE_ORIGEN_REAL] + params_alc)
        resultado = cursor.fetchone()
        conn.close()

        if resultado and resultado[0]:
            return f"🏭 Packing {packing.upper()}: {formatear_kg(resultado[0])} kg recibidos (real), en {resultado[1]} variedades"
        else:
            return f"No encontré cosecha real registrada del packing '{packing}'"
    except Exception as e:
        logger.error(f"Error en obtener_resumen_por_packing: {str(e)}")
        return f"Error al consultar: {str(e)}"

def formatear_comparativo_estimaciones(filas, fecha_inicio, fecha_fin, filtro_desc="", unidad="kg"):
    """
    filas: lista de tuplas (Variedad, Base Origen, total).
    Tabla Variedad | Invierno | Primavera | Real, más diferencias y % (Real vs cada
    estimación, y cómo cambió la estimación de Invierno a Primavera).
    unidad: etiqueta del total (ej. "kg", "BINS") según qué columna se sumó.
    """
    datos = {}
    for variedad, base_origen, total in filas:
        if not total:
            continue
        v = (variedad or "").strip().upper()
        if base_origen == BASE_ORIGEN_ESTIM_INVIERNO:
            clave = "invierno"
        elif base_origen == BASE_ORIGEN_ESTIM_PRIMAVERA:
            clave = "primavera"
        else:
            clave = "real"
        datos.setdefault(v, {})
        datos[v][clave] = datos[v].get(clave, 0) + total

    if not datos:
        return None

    rango = fecha_inicio if fecha_inicio == fecha_fin else f"{fecha_inicio} a {fecha_fin}"
    lineas = [f"📊 Comparativo estimaciones {rango}{filtro_desc}:"]

    etiqueta_inv = ETIQUETA_FUENTE[BASE_ORIGEN_ESTIM_INVIERNO]
    etiqueta_prim = ETIQUETA_FUENTE[BASE_ORIGEN_ESTIM_PRIMAVERA]
    anchos = {"variedad": 18, "num": 11}
    header = (
        f"{'Variedad':<{anchos['variedad']}}{etiqueta_inv:>{anchos['num']}}"
        f"{etiqueta_prim:>{anchos['num']}}{'Real':>{anchos['num']}}"
    )
    filas_tabla = [header]

    tot_inv = tot_prim = tot_real = 0
    for variedad in sorted(datos.keys()):
        d = datos[variedad]
        inv = d.get("invierno", 0)
        prim = d.get("primavera", 0)
        real = d.get("real", 0)
        tot_inv += inv
        tot_prim += prim
        tot_real += real
        filas_tabla.append(
            f"{_truncar(variedad, anchos['variedad']):<{anchos['variedad']}}"
            f"{formatear_kg(inv) if inv else '-':>{anchos['num']}}"
            f"{formatear_kg(prim) if prim else '-':>{anchos['num']}}"
            f"{formatear_kg(real) if real else '-':>{anchos['num']}}"
        )
    filas_tabla.append("-" * (anchos["variedad"] + anchos["num"] * 3))
    filas_tabla.append(
        f"{'TOTAL':<{anchos['variedad']}}"
        f"{formatear_kg(tot_inv):>{anchos['num']}}"
        f"{formatear_kg(tot_prim):>{anchos['num']}}"
        f"{formatear_kg(tot_real):>{anchos['num']}}"
    )
    lineas.append(f"```{chr(10).join(filas_tabla)}```")

    def variacion(a, b, etiqueta):
        if not b:
            return None
        pct = ((a - b) / b) * 100
        signo = "+" if pct >= 0 else ""
        return f"{etiqueta}: {signo}{pct:.1f}% ({signo}{formatear_kg(a - b)} {unidad})"

    resumen = [f"\n📦 Totales: {etiqueta_inv} {formatear_kg(tot_inv)} {unidad} · {etiqueta_prim} {formatear_kg(tot_prim)} {unidad} · Real {formatear_kg(tot_real)} {unidad}"]
    for texto in [
        variacion(tot_real, tot_inv, f"Real vs {etiqueta_inv}"),
        variacion(tot_real, tot_prim, f"Real vs {etiqueta_prim}"),
        variacion(tot_prim, tot_inv, f"{etiqueta_prim} vs {etiqueta_inv}"),
    ]:
        if texto:
            resumen.append(texto)

    lineas.append("\n".join(resumen))
    return "\n".join(lineas)

def obtener_comparativo_estimaciones(especie=None, variedad=None, productor=None, packing=None, fecha_inicio=None, fecha_fin=None, envase=None, temporada=None, alcance=None):
    """
    Comparativo Estim Invierno vs Estim Primavera vs Real (Recepción Planta), agrupado por
    variedad, con diferencias y %. Por defecto usa toda la temporada vigente completa (no solo
    hasta hoy), ya que las estimaciones cubren la temporada entera. Si se da temporada (ej. 2025
    para "la temporada pasada"), usa el rango de esa temporada en vez de la vigente.
    Si se da envase (ej. "BINS"), compara en esa unidad usando el Bultos real que tiene guardado
    cada una de las tres fuentes (Invierno, Primavera y Real), no un factor calculado.
    """
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        if not fecha_inicio or not fecha_fin:
            temporada_rango = temporada or TEMPORADA_ACTUAL
            if not temporada_rango:
                conn.close()
                return "No pude determinar el rango de esa temporada. ¿Puedes darme una fecha o rango específico?"
            inicio_calc, fin_calc = rango_temporada(temporada_rango)
            fecha_inicio = fecha_inicio or inicio_calc.strftime("%Y-%m-%d")
            fecha_fin = fecha_fin or fin_calc.strftime("%Y-%m-%d")

        try:
            datetime.strptime(fecha_inicio, "%Y-%m-%d")
            datetime.strptime(fecha_fin, "%Y-%m-%d")
        except (ValueError, TypeError):
            conn.close()
            return "No entendí el rango de fechas. ¿Puedes indicarlo como 'entre el DD-MM-YYYY y el DD-MM-YYYY'?"

        condiciones = ["[Base Origen] IN (?, ?, ?)", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [BASE_ORIGEN_ESTIM_INVIERNO, BASE_ORIGEN_ESTIM_PRIMAVERA, BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
        sql_alc, params_alc = _sql_alcance(alcance)
        if sql_alc:
            condiciones.append(sql_alc)
            params.extend(params_alc)
        filtro_desc = ""
        if especie:
            condiciones.append("Especie LIKE ?")
            params.append(f"%{especie}%")
            filtro_desc += f" de {traducir_especie(especie)}"
        if variedad:
            condiciones.append("Variedad LIKE ?")
            params.append(f"%{variedad}%")
            filtro_desc += f" ({variedad.upper()})"
        if productor:
            condiciones.append("Productor LIKE ?")
            params.append(f"%{productor}%")
            filtro_desc += f" del productor {productor.upper()}"
        if packing:
            condiciones.append("Packing LIKE ?")
            params.append(f"%{packing}%")
            filtro_desc += f" en {packing.upper()}"

        columna_suma = "KgsRecepcionados"
        unidad = "kg"
        if envase:
            condiciones.append("Envase LIKE ?")
            params.append(f"%{envase}%")
            filtro_desc += f" en {envase.upper()}"
            columna_suma = "Bultos"
            unidad = envase.upper()
            # Hay filas duplicadas en la base para el mismo registro: una con Bultos y
            # KgsRecepcionados=NULL, y otra con ambos. Sin este filtro, sumar Bultos duplica
            # el total (verificado: 590 en vez de 295, exactamente el doble).
            condiciones.append("KgsRecepcionados IS NOT NULL")

        where = " AND ".join(condiciones)
        query = f"""
            SELECT Variedad, [Base Origen], SUM({columna_suma}) as total
            FROM [Recepcion_Consolidada]
            WHERE {where}
            GROUP BY Variedad, [Base Origen]
            HAVING SUM({columna_suma}) > 0
        """
        cursor.execute(query, params)
        filas = cursor.fetchall()
        conn.close()

        resultado = formatear_comparativo_estimaciones(filas, fecha_inicio, fecha_fin, filtro_desc, unidad)
        if not resultado:
            return f"No hay datos de estimaciones ni cosecha real{filtro_desc} entre {fecha_inicio} y {fecha_fin}"
        return resultado
    except Exception as e:
        logger.error(f"Error en obtener_comparativo_estimaciones: {str(e)}")
        return f"Error al consultar: {str(e)}"

UMBRAL_DIAS_TABLA_DETALLADA = 14
MAX_GRUPOS_TABLA = 8

def _fecha_str(fecha):
    return fecha.strftime("%d-%m-%Y") if hasattr(fecha, "strftime") else str(fecha)

def _truncar(texto, ancho):
    texto = texto or ""
    if len(texto) <= ancho:
        return texto
    return texto[:ancho - 1] + "…"

def _formatear_grupo(v):
    """Varios grupos comparten prefijo (ej. 'AGUA SANTA (P Y)', 'AGUA SANTA (Garcia P)'),
    lo que los hacía indistinguibles al truncar la columna. Se muestra solo lo que está
    entre paréntesis cuando existe, que es la parte que realmente los diferencia."""
    texto = str(v).strip() if v is not None else ""
    inicio = texto.find("(")
    fin = texto.find(")", inicio + 1)
    if inicio != -1 and fin != -1:
        return texto[inicio + 1:fin].strip()
    return texto

def formatear_cosecha_detalle(filas, fecha_inicio, fecha_fin, filtro_desc="", mostrar_fechas=True, unidad="kg", base_estimado=None):
    """
    filas: lista de tuplas (Packing, Productor, Especie, Variedad, Fecha, Base Origen, total).
    Si el rango es corto y hay pocos grupos, arma una tabla de fecha x (estimado, real) por
    cada packing/productor/variedad. Si no, colapsa a una tabla única con columnas
    Planta | Productor | Especie | Variedad | Estimado | Real.
    unidad: etiqueta del total (ej. "kg", "BINS", "TOTES") según qué columna se sumó.
    base_estimado: valor real de [Base Origen] que cuenta como "estimado" en estas filas
    (Estim Primavera por defecto, o Trisemanal/Estim Invierno si se pidió explícitamente).
    """
    if not filas:
        return None

    base_estimado = base_estimado or BASE_ORIGEN_ESTIMADO
    etiqueta_estimado = ETIQUETA_FUENTE.get(base_estimado, "Estimado")
    grupos = {}  # (packing, productor, especie, variedad) -> {fecha: {"estimado":x, "real":y}}
    for packing, productor, especie, variedad, fecha, base_origen, total in filas:
        if not total:
            continue
        # Normaliza mayúsculas: la misma planta/productor puede venir escrito distinto
        # según si la fila es de estimado o de 'Recepción Planta' en la base de origen.
        clave = (
            (packing or "").strip().upper(),
            (productor or "").strip().upper(),
            (especie or "").strip().upper(),
            (variedad or "").strip().upper(),
        )
        clave_valor = "estimado" if base_origen == base_estimado else "real"
        grupos.setdefault(clave, {}).setdefault(fecha, {})[clave_valor] = total

    if not grupos:
        return None

    rango = fecha_inicio if fecha_inicio == fecha_fin else f"{fecha_inicio} a {fecha_fin}"
    lineas = [f"📅 Detalle {rango}{filtro_desc}:"]

    tabla_completa = mostrar_fechas and len(grupos) <= MAX_GRUPOS_TABLA
    total_estimado_gral = 0
    total_real_gral = 0

    if tabla_completa:
        for (packing, productor, especie, variedad), fechas in sorted(grupos.items()):
            total_est = sum(v.get("estimado", 0) or 0 for v in fechas.values())
            total_real = sum(v.get("real", 0) or 0 for v in fechas.values())
            total_estimado_gral += total_est
            total_real_gral += total_real

            filas_tabla = [f"{'Fecha':<11}{etiqueta_estimado:>10}{'Real':>9}"]
            for fecha in sorted(fechas.keys()):
                vals = fechas[fecha]
                est = vals.get("estimado", 0) or 0
                real = vals.get("real", 0) or 0
                est_str = formatear_kg(est) if est else "-"
                real_str = formatear_kg(real) if real else "-"
                filas_tabla.append(f"{_fecha_str(fecha):<11}{est_str:>10}{real_str:>9}")
            filas_tabla.append("-" * 30)
            filas_tabla.append(f"{'TOTAL':<11}{formatear_kg(total_est):>10}{formatear_kg(total_real):>9}")

            tabla_texto = "\n".join(filas_tabla)
            lineas.append(f"\n🏭 {packing} · {productor} · {variedad}\n```{tabla_texto}```")
    else:
        # Planta y Productor van como encabezado (nombre completo, sin cortar).
        # La tabla queda angosta (Variedad | Estimado | Real) para que siempre entre bien.
        por_planta_productor = {}
        for (packing, productor, especie, variedad), fechas in grupos.items():
            total_est = sum(v.get("estimado", 0) or 0 for v in fechas.values())
            total_real = sum(v.get("real", 0) or 0 for v in fechas.values())
            total_estimado_gral += total_est
            total_real_gral += total_real
            clave_pp = (packing, productor)
            por_planta_productor.setdefault(clave_pp, []).append((especie, variedad, total_est, total_real))

        anchos = {"productor": 45, "especie": 8, "variedad": 18, "num": 10}
        ancho_total = anchos["productor"] + anchos["especie"] + anchos["variedad"] + anchos["num"] * 2
        MAX_SECCIONES = 20
        secciones_mostradas = 0
        total_secciones = len(por_planta_productor)

        planta_actual = None
        for (packing, productor), items in sorted(por_planta_productor.items()):
            if secciones_mostradas >= MAX_SECCIONES:
                break
            secciones_mostradas += 1

            if packing != planta_actual:
                planta_actual = packing
                lineas.append(f"\n🏭 *{packing}*")

            lineas.append(f"\n{productor}")

            filas_tabla = [
                f"{'Especie':<{anchos['especie']}}{'Variedad':<{anchos['variedad']}}"
                f"{etiqueta_estimado:>{anchos['num']}}{'Real':>{anchos['num']}}"
            ]
            sub_est = 0
            sub_real = 0
            for especie, variedad, total_est, total_real in sorted(items, key=lambda x: (x[0], x[1])):
                sub_est += total_est
                sub_real += total_real
                est_str = formatear_kg(total_est) if total_est else "-"
                real_str = formatear_kg(total_real) if total_real else "-"
                filas_tabla.append(
                    f"{_truncar(traducir_especie(especie), anchos['especie']):<{anchos['especie']}}"
                    f"{_truncar(variedad, anchos['variedad']):<{anchos['variedad']}}"
                    f"{est_str:>{anchos['num']}}{real_str:>{anchos['num']}}"
                )
            if len(items) > 1:
                filas_tabla.append("-" * ancho_total)
                filas_tabla.append(
                    f"{'TOTAL':<{anchos['especie'] + anchos['variedad']}}"
                    f"{formatear_kg(sub_est):>{anchos['num']}}{formatear_kg(sub_real):>{anchos['num']}}"
                )

            lineas.append(f"```{chr(10).join(filas_tabla)}```")

        if total_secciones > MAX_SECCIONES:
            lineas.append(f"\n(mostrando {MAX_SECCIONES} de {total_secciones} productores — acota la consulta para ver el resto)")

    lineas.append(
        f"\n📦 Total general: {etiqueta_estimado} {formatear_kg(total_estimado_gral)} {unidad}, "
        f"real {formatear_kg(total_real_gral)} {unidad}"
    )
    return "\n".join(lineas)

def obtener_cosecha_detalle(fecha_inicio=None, fecha_fin=None, especie=None, variedad=None, productor=None, packing=None, grupo=None, forzar_fechas=False, envase=None, temporada=None, fuente_estimado=None, alcance=None):
    """
    Detalle de cosecha entre fecha_inicio y fecha_fin (o solo fecha_inicio si no hay fecha_fin),
    con columnas de estimado y real (Recepción Planta) por fecha, agrupado por
    packing/productor/variedad. Filtros opcionales. Si el rango es muy amplio o hay muchos
    grupos, se colapsa a solo totales para no saturar el mensaje.
    Por defecto el "estimado" es Estim Primavera; fuente_estimado puede forzar 'trisemanal'
    o 'invierno' explícitamente.
    Si no se da fecha_inicio, se usa el inicio de la temporada vigente hasta hoy (para
    preguntas tipo "toda la temporada", "hasta hoy", sin fechas explícitas). Si se da temporada
    (ej. 2025 para "la temporada pasada") sin fechas, se usa el rango completo de esa temporada.
    Si se da envase (ej. "BINS", "TOTES", "CAJA EQ"), se filtra por ese tipo de envase y se
    suma la cantidad real de unidades (Bultos) en vez de kilos, sin inventar factores de
    conversión (los factores kg/unidad no son confiables en los datos históricos).
    """
    try:
        base_estimado = resolver_fuente_estimado(fuente_estimado)
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        if not fecha_inicio:
            if temporada:
                inicio_calc, fin_calc = rango_temporada(temporada)
                fecha_inicio = inicio_calc.strftime("%Y-%m-%d")
                fecha_fin = fin_calc.strftime("%Y-%m-%d")
            else:
                if not TEMPORADA_ACTUAL:
                    conn.close()
                    return "No pude determinar el inicio de temporada. ¿Puedes darme una fecha o rango específico?"
                inicio_calc, _ = rango_temporada(TEMPORADA_ACTUAL)
                fecha_inicio = inicio_calc.strftime("%Y-%m-%d")
                fecha_fin = datetime.now().strftime("%Y-%m-%d")
        elif not fecha_fin:
            fecha_fin = fecha_inicio

        try:
            datetime.strptime(fecha_inicio, "%Y-%m-%d")
            datetime.strptime(fecha_fin, "%Y-%m-%d")
        except (ValueError, TypeError):
            conn.close()
            return "No entendí el rango de fechas. ¿Puedes indicarlo como 'entre el DD-MM-YYYY y el DD-MM-YYYY'?"

        condiciones = ["[Base Origen] IN (?, ?)", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [base_estimado, BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
        sql_alc, params_alc = _sql_alcance(alcance)
        if sql_alc:
            condiciones.append(sql_alc)
            params.extend(params_alc)
        filtro_desc = ""
        if especie:
            condiciones.append("Especie LIKE ?")
            params.append(f"%{especie}%")
            filtro_desc += f" de {traducir_especie(especie)}"
        if variedad:
            condiciones.append("Variedad LIKE ?")
            params.append(f"%{variedad}%")
            filtro_desc += f" ({variedad.upper()})"
        if productor:
            condiciones.append("Productor LIKE ?")
            params.append(f"%{productor}%")
            filtro_desc += f" del productor {productor.upper()}"
        if packing:
            condiciones.append("Packing LIKE ?")
            params.append(f"%{packing}%")
            filtro_desc += f" en {packing.upper()}"
        if grupo:
            condiciones.append("Grupo LIKE ?")
            params.append(f"%{grupo}%")
            filtro_desc += f" del grupo {grupo.upper()}"

        columna_suma = "KgsRecepcionados"
        unidad = "kg"
        if envase:
            condiciones.append("Envase LIKE ?")
            params.append(f"%{envase}%")
            filtro_desc += f" en {envase.upper()}"
            columna_suma = "Bultos"
            unidad = envase.upper()
            # Hay filas duplicadas en la base para el mismo registro: una con Bultos y
            # KgsRecepcionados=NULL, y otra con ambos. Sin este filtro, sumar Bultos duplica
            # el total (verificado: 590 en vez de 295, exactamente el doble).
            condiciones.append("KgsRecepcionados IS NOT NULL")

        where = " AND ".join(condiciones)
        query = f"""
            SELECT Packing, Productor, Especie, Variedad, CAST(Fecha AS DATE) as Fecha, [Base Origen],
                   SUM({columna_suma}) as total
            FROM [Recepcion_Consolidada]
            WHERE {where}
            GROUP BY Packing, Productor, Especie, Variedad, CAST(Fecha AS DATE), [Base Origen]
            HAVING SUM({columna_suma}) > 0
            ORDER BY Packing, Productor, Variedad, Fecha
        """
        cursor.execute(query, params)
        filas = cursor.fetchall()
        conn.close()

        try:
            dias_rango = (datetime.strptime(fecha_fin, "%Y-%m-%d") - datetime.strptime(fecha_inicio, "%Y-%m-%d")).days
        except ValueError:
            dias_rango = 0
        mostrar_fechas = forzar_fechas or dias_rango <= UMBRAL_DIAS_TABLA_DETALLADA

        resultado = formatear_cosecha_detalle(filas, fecha_inicio, fecha_fin, filtro_desc, mostrar_fechas, unidad, base_estimado)
        if not resultado:
            return f"No hay datos registrados{filtro_desc} entre {fecha_inicio} y {fecha_fin}"
        return resultado
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_detalle: {str(e)}")
        return f"Error al consultar: {str(e)}"

DIMENSIONES_SQL = {
    "especie": "Especie",
    "variedad": "Variedad",
    "productor": "Productor",
    "packing": "Packing",
    "grupo": "Grupo",
    "fecha": "CAST(Fecha AS DATE)",
    "semana": "CAST(Semana AS INT)",
}
DIMENSIONES_ETIQUETA = {
    "especie": "Especie",
    "variedad": "Variedad",
    "productor": "Productor",
    "packing": "Planta",
    "grupo": "Grupo",
    "fecha": "Fecha",
    "semana": "Semana",
}
MAX_FILAS_FLEXIBLE = 60

def formatear_cosecha_flexible(filas, dimensiones, fecha_inicio, fecha_fin, filtro_desc="", unidad="kg", base_estimado=None):
    """
    filas: tuplas (valor_dim1, valor_dim2, ..., Base Origen, total), según 'dimensiones'.
    Arma UNA sola tabla con columnas = dimensiones pedidas + Estimado + Real, y fila TOTAL.
    base_estimado: valor real de [Base Origen] que cuenta como "estimado" en estas filas
    (Estim Primavera por defecto, o Trisemanal/Estim Invierno si se pidió explícitamente).
    """
    if not filas:
        return None

    base_estimado = base_estimado or BASE_ORIGEN_ESTIMADO
    etiqueta_estimado = ETIQUETA_FUENTE.get(base_estimado, "Estimado")
    n = len(dimensiones)
    datos = {}
    for fila in filas:
        valores_dim = fila[:n]
        base_origen = fila[n]
        total = fila[n + 1]
        if not total:
            continue
        clave = tuple(
            (v.strip().upper() if isinstance(v, str) else v) for v in valores_dim
        )
        tipo = "estimado" if base_origen == base_estimado else "real"
        datos.setdefault(clave, {})
        datos[clave][tipo] = datos[clave].get(tipo, 0) + total

    if not datos:
        return None

    rango = fecha_inicio if fecha_inicio == fecha_fin else f"{fecha_inicio} a {fecha_fin}"
    lineas = [f"📅 Resumen {rango}{filtro_desc}:"]

    if n == 0:
        # Sin dimensiones: el usuario pidió solo el total, sin desglose.
        tot_est = sum(v.get("estimado", 0) for v in datos.values())
        tot_real = sum(v.get("real", 0) for v in datos.values())
        lineas.append(f"\n📦 Total: {etiqueta_estimado} {formatear_kg(tot_est)} {unidad}, real {formatear_kg(tot_real)} {unidad}")
        return "\n".join(lineas)

    anchos_dim = []
    for d in dimensiones:
        if d == "semana":
            anchos_dim.append(8)
        elif d == "fecha":
            anchos_dim.append(11)
        elif d in ("productor", "packing"):
            anchos_dim.append(16)
        else:
            anchos_dim.append(13)
    ancho_num = 11

    header = "".join(
        f"{DIMENSIONES_ETIQUETA.get(d, d):<{a}}" for d, a in zip(dimensiones, anchos_dim)
    )
    header += f"{etiqueta_estimado:>{ancho_num}}{'Real':>{ancho_num}}"
    filas_tabla = [header]

    def clave_orden(clave):
        return [
            orden_semana(v) if d == "semana" and v is not None else str(v)
            for v, d in zip(clave, dimensiones)
        ]

    tot_est = 0
    tot_real = 0
    claves_ordenadas = sorted(datos.keys(), key=clave_orden)
    truncado = len(claves_ordenadas) > MAX_FILAS_FLEXIBLE
    for clave in claves_ordenadas[:MAX_FILAS_FLEXIBLE]:
        vals = datos[clave]
        est = vals.get("estimado", 0)
        real = vals.get("real", 0)
        tot_est += est
        tot_real += real
        fila_texto = ""
        for v, a, d in zip(clave, anchos_dim, dimensiones):
            if d == "fecha":
                texto = _fecha_str(v)
            elif d == "semana":
                texto = etiqueta_semana(v) if v is not None else "-"
            elif d == "especie":
                texto = traducir_especie(v)
            elif d == "grupo":
                texto = _formatear_grupo(v)
            else:
                texto = str(v) if v is not None else ""
            fila_texto += f"{_truncar(texto, a):<{a}}"
        est_str = formatear_kg(est) if est else "-"
        real_str = formatear_kg(real) if real else "-"
        fila_texto += f"{est_str:>{ancho_num}}{real_str:>{ancho_num}}"
        filas_tabla.append(fila_texto)

    # Si se truncó, los totales igual deben sumar TODAS las filas, no solo las mostradas
    if truncado:
        for clave in claves_ordenadas[MAX_FILAS_FLEXIBLE:]:
            vals = datos[clave]
            tot_est += vals.get("estimado", 0)
            tot_real += vals.get("real", 0)

    ancho_total = sum(anchos_dim) + ancho_num * 2
    filas_tabla.append("-" * ancho_total)
    filas_tabla.append(
        f"{'TOTAL':<{sum(anchos_dim)}}{formatear_kg(tot_est):>{ancho_num}}{formatear_kg(tot_real):>{ancho_num}}"
    )

    lineas.append(f"```{chr(10).join(filas_tabla)}```")
    if truncado:
        lineas.append(f"\n(mostrando {MAX_FILAS_FLEXIBLE} de {len(claves_ordenadas)} filas — acota la consulta para ver el resto; los totales sí incluyen todo)")

    lineas.append(
        f"\n📦 Total general: {etiqueta_estimado} {formatear_kg(tot_est)} {unidad}, real {formatear_kg(tot_real)} {unidad}"
    )
    return "\n".join(lineas)

def obtener_cosecha_flexible(agrupar_por, fecha_inicio=None, fecha_fin=None, especie=None, variedad=None,
                              productor=None, packing=None, grupo=None, envase=None, temporada=None,
                              fuente_estimado=None, kg_por_caja_eq=None, alcance=None):
    """
    Consulta genérica: agrupa por las dimensiones exactas que se pidan (cualquier combinación
    de especie/variedad/productor/packing/grupo/fecha, o ninguna si se pide solo el total),
    sumando estimado y real (Recepción Planta) o Bultos si se da envase. Por defecto el
    "estimado" es Estim Primavera; fuente_estimado puede forzar 'trisemanal' o 'invierno'
    explícitamente. A diferencia de las demás consultas, si no se da ningún periodo (ni fechas
    ni temporada) NO asume nada: pide que se aclare el periodo.
    """
    try:
        base_estimado = resolver_fuente_estimado(fuente_estimado)
        dimensiones = [d for d in (agrupar_por or []) if d in DIMENSIONES_SQL]

        if not fecha_inicio and not temporada:
            return "¿Para qué periodo necesitas este dato? (por ejemplo: esta temporada, un rango de fechas específico, o solo hoy)"

        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        if not fecha_inicio:
            inicio_calc, fin_calc = rango_temporada(temporada)
            fecha_inicio = inicio_calc.strftime("%Y-%m-%d")
            fecha_fin = fin_calc.strftime("%Y-%m-%d")
        elif not fecha_fin:
            fecha_fin = fecha_inicio

        try:
            datetime.strptime(fecha_inicio, "%Y-%m-%d")
            datetime.strptime(fecha_fin, "%Y-%m-%d")
        except (ValueError, TypeError):
            conn.close()
            return "No entendí el rango de fechas. ¿Puedes indicarlo como 'entre el DD-MM-YYYY y el DD-MM-YYYY'?"

        condiciones = ["[Base Origen] IN (?, ?)", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [base_estimado, BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
        sql_alc, params_alc = _sql_alcance(alcance)
        if sql_alc:
            condiciones.append(sql_alc)
            params.extend(params_alc)
        filtro_desc = ""
        if especie:
            condiciones.append("Especie LIKE ?")
            params.append(f"%{especie}%")
            filtro_desc += f" de {traducir_especie(especie)}"
        if variedad:
            condiciones.append("Variedad LIKE ?")
            params.append(f"%{variedad}%")
            filtro_desc += f" ({variedad.upper()})"
        if productor:
            condiciones.append("Productor LIKE ?")
            params.append(f"%{productor}%")
            filtro_desc += f" del productor {productor.upper()}"
        if packing:
            condiciones.append("Packing LIKE ?")
            params.append(f"%{packing}%")
            filtro_desc += f" en {packing.upper()}"
        if grupo:
            condiciones.append("Grupo LIKE ?")
            params.append(f"%{grupo}%")
            filtro_desc += f" del grupo {grupo.upper()}"

        columna_suma = "KgsRecepcionados"
        unidad = "kg"
        convertir_a_cajas = None
        if envase:
            # "Caja Eq" solo existe en la base para algunas especies/variedades (uva y una
            # pera). Para el resto no hay bultos que sumar: se consulta en kilos y se
            # convierte con el factor que dé el usuario, porque los kg/caja que se podrían
            # derivar de la base son inconsistentes (de 8 a 254.000 kg/caja por filas dobles).
            if _es_caja_eq(envase) and not hay_datos_caja_eq(
                conn, fecha_inicio, fecha_fin, especie, variedad, productor, packing, grupo,
                alcance=alcance
            ):
                if not kg_por_caja_eq:
                    conn.close()
                    return _pregunta_kg_por_caja_eq(variedad or especie)
                try:
                    convertir_a_cajas = float(kg_por_caja_eq)
                except (TypeError, ValueError):
                    conn.close()
                    return _pregunta_kg_por_caja_eq(variedad or especie)
                if convertir_a_cajas <= 0:
                    conn.close()
                    return _pregunta_kg_por_caja_eq(variedad or especie)
                unidad = "CAJA EQ"
                filtro_desc += f" en CAJA EQ (a {formatear_kg(convertir_a_cajas)} kg por caja)"
            else:
                condiciones.append("Envase LIKE ?")
                params.append(f"%{envase}%")
                filtro_desc += f" en {envase.upper()}"
                columna_suma = "Bultos"
                unidad = envase.upper()
                condiciones.append("KgsRecepcionados IS NOT NULL")

        where = " AND ".join(condiciones)
        columnas_sql = ", ".join(DIMENSIONES_SQL[d] for d in dimensiones)
        select_cols = f"{columnas_sql}, " if columnas_sql else ""
        group_cols = f"{columnas_sql}, " if columnas_sql else ""
        query = f"""
            SELECT {select_cols}[Base Origen], SUM({columna_suma}) as total
            FROM [Recepcion_Consolidada]
            WHERE {where}
            GROUP BY {group_cols}[Base Origen]
            HAVING SUM({columna_suma}) > 0
        """
        cursor.execute(query, params)
        filas = cursor.fetchall()
        conn.close()

        if convertir_a_cajas:
            # Los kilos consultados se pasan a cajas equivalentes con el factor del usuario.
            filas = [
                tuple(fila[:-1]) + ((fila[-1] or 0) / convertir_a_cajas,)
                for fila in filas
            ]

        resultado = formatear_cosecha_flexible(filas, dimensiones, fecha_inicio, fecha_fin, filtro_desc, unidad, base_estimado)
        if not resultado:
            return f"No hay datos registrados{filtro_desc} entre {fecha_inicio} y {fecha_fin}"
        return resultado
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_flexible: {str(e)}")
        return f"Error al consultar: {str(e)}"

def _es_caja_eq(envase):
    """True si el envase pedido es la unidad 'Caja Eq' (tolera 'cajas eq', 'CAJA EQ', etc.)."""
    if not envase:
        return False
    texto = str(envase).upper().replace(".", "").strip()
    return "CAJA" in texto and "EQ" in texto

def _pregunta_kg_por_caja_eq(que):
    """Pregunta que se le devuelve al usuario cuando falta el factor de conversión."""
    nombre = (que or "consultada").upper()
    return (
        f'Para la variedad "{nombre}" consultada, ¿cuántos kilos equivalen a una Caja Eq?\n\n'
        "Esa variedad no tiene Caja Eq registrada como envase en el sistema, así que necesito "
        "el equivalente para hacer la conversión desde kilos."
    )

def hay_datos_caja_eq(conn, fecha_inicio, fecha_fin, especie=None, variedad=None,
                      productor=None, packing=None, grupo=None, alcance=None):
    """¿Hay filas con envase CAJA EQ para esos filtros y periodo? Si las hay se consulta
    directo la columna Bultos; si no, hay que convertir desde kilos."""
    try:
        condiciones = ["Envase LIKE '%CAJA EQ%'", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [fecha_inicio, fecha_fin]
        sql_alc, params_alc = _sql_alcance(alcance)
        if sql_alc:
            condiciones.append(sql_alc)
            params.extend(params_alc)
        for columna, valor in (
            ("Especie", especie), ("Variedad", variedad), ("Productor", productor),
            ("Packing", packing), ("Grupo", grupo),
        ):
            if valor:
                condiciones.append(f"{columna} LIKE ?")
                params.append(f"%{valor}%")
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT TOP 1 1 FROM [Recepcion_Consolidada] WHERE {' AND '.join(condiciones)}",
            params,
        )
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Error verificando datos de CAJA EQ: {str(e)}")
        return False

def _obtener_extremo_cosecha(especie, variedad, packing, productor, temporada, usar_maximo, alcance=None):
    """Función compartida: encuentra la primera (MIN) o última (MAX) fecha con cosecha real."""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        condiciones = ["[Base Origen] = ?", "KgsRecepcionados > 0"]
        params = [BASE_ORIGEN_REAL]
        sql_alc, params_alc = _sql_alcance(alcance)
        if sql_alc:
            condiciones.append(sql_alc)
            params.extend(params_alc)
        if especie:
            condiciones.append("Especie LIKE ?")
            params.append(f"%{especie}%")
        if variedad:
            condiciones.append("Variedad LIKE ?")
            params.append(f"%{variedad}%")
        if packing:
            condiciones.append("Packing LIKE ?")
            params.append(f"%{packing}%")
        if productor:
            condiciones.append("Productor LIKE ?")
            params.append(f"%{productor}%")
        if temporada:
            condiciones.append("Temporada = ?")
            params.append(temporada)
        where = " AND ".join(condiciones)

        funcion_sql = "MAX" if usar_maximo else "MIN"
        cursor.execute(f"SELECT {funcion_sql}(CAST(Fecha AS DATE)) FROM [Recepcion_Consolidada] WHERE {where}", params)
        fila = cursor.fetchone()
        conn.close()

        fecha_encontrada = fila[0] if fila else None
        if not fecha_encontrada:
            referencia = especie or variedad or packing or productor or ""
            temp_desc = f" en la temporada {temporada}" if temporada else ""
            return f"No encontré cosecha real registrada de {referencia}{temp_desc}" if referencia else f"No encontré cosecha real registrada{temp_desc}"

        fecha_str = fecha_encontrada.strftime("%Y-%m-%d") if hasattr(fecha_encontrada, "strftime") else str(fecha_encontrada)
        return obtener_cosecha_detalle(fecha_str, fecha_str, especie=especie, variedad=variedad, packing=packing, productor=productor)
    except Exception as e:
        logger.error(f"Error en _obtener_extremo_cosecha: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_ultima_cosecha(especie=None, variedad=None, packing=None, productor=None, temporada=None, alcance=None):
    """Encuentra la última (más reciente) fecha con cosecha real, y muestra su detalle"""
    return _obtener_extremo_cosecha(especie, variedad, packing, productor, temporada, usar_maximo=True, alcance=alcance)

def obtener_primera_cosecha(especie=None, variedad=None, packing=None, productor=None, temporada=None, alcance=None):
    """
    Encuentra la primera fecha con cosecha real (cuándo empezó), y muestra su detalle.
    Si no se especifica temporada, se limita a la temporada vigente por defecto (a diferencia
    de "última cosecha", "primera cosecha" sin temporada casi siempre implica "esta temporada",
    no la primera vez registrada en toda la historia).
    """
    temporada = temporada or TEMPORADA_ACTUAL
    return _obtener_extremo_cosecha(especie, variedad, packing, productor, temporada, usar_maximo=False, alcance=alcance)

# ============================================================================
# PROCESAMIENTO DE MENSAJES
# ============================================================================

def cargar_variedades_conocidas():
    """
    Carga la lista real de variedades desde la base de datos, para que Claude pueda reconocer
    lo que escribe el usuario (con typos, sin tildes, abreviado) y usar el nombre exacto de la BD.
    """
    try:
        conn = conectar_sql()
        if not conn:
            return []
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT [Variedad Agronomica] FROM [PKG-Cos-Estima] WHERE [Variedad Agronomica] IS NOT NULL
            UNION
            SELECT DISTINCT Variedad FROM [Recepcion_Consolidada] WHERE Variedad IS NOT NULL
        """)
        variedades = sorted(set(row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()))
        conn.close()
        logger.info(f"Cargadas {len(variedades)} variedades conocidas")
        return variedades
    except Exception as e:
        logger.error(f"Error cargando variedades conocidas: {str(e)}")
        return []

VARIEDADES_CONOCIDAS = cargar_variedades_conocidas()

# Alias de plantas/packing conocidos, dados por el usuario (Agua Santa)
ALIAS_PACKING = {
    "PACKING SANTA ANA DEL HUIQUE": ["Planta Santa Ana", "Packing Santa Ana", "Santa Ana"],
    "PLANTA ALMAHUE": ["Almahue"],
    "PLANTA EL CARMELO": ["El Carmelo"],
    "PLANTA EL PARQUE": ["El Parque"],
    "PLANTA LISONJERA": ["Lisonjera"],
}

def cargar_packings_conocidos():
    """Carga los nombres reales de Packing/Planta desde la base de datos"""
    try:
        conn = conectar_sql()
        if not conn:
            return []
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Packing FROM [Recepcion_Consolidada] WHERE Packing IS NOT NULL")
        packings = sorted(set(row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()))
        conn.close()
        logger.info(f"Cargados {len(packings)} packings conocidos")
        return packings
    except Exception as e:
        logger.error(f"Error cargando packings conocidos: {str(e)}")
        return []

PACKINGS_CONOCIDOS = cargar_packings_conocidos()

def cargar_envases_conocidos():
    """Carga los tipos de envase reales desde la base de datos (bins, totes, cajas, etc.)"""
    try:
        conn = conectar_sql()
        if not conn:
            return []
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Envase FROM [Recepcion_Consolidada] WHERE Envase IS NOT NULL")
        envases = sorted(set(row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()))
        conn.close()
        logger.info(f"Cargados {len(envases)} tipos de envase conocidos")
        return envases
    except Exception as e:
        logger.error(f"Error cargando envases conocidos: {str(e)}")
        return []

ENVASES_CONOCIDOS = cargar_envases_conocidos()

def cargar_productores_conocidos():
    """Carga los nombres reales de productor desde la base de datos"""
    try:
        conn = conectar_sql()
        if not conn:
            return []
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Productor FROM [Recepcion_Consolidada] WHERE Productor IS NOT NULL")
        productores = sorted(set(row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()))
        conn.close()
        logger.info(f"Cargados {len(productores)} productores conocidos")
        return productores
    except Exception as e:
        logger.error(f"Error cargando productores conocidos: {str(e)}")
        return []

PRODUCTORES_CONOCIDOS = cargar_productores_conocidos()

def cargar_grupos_conocidos():
    """Carga los nombres reales de grupo (holding/empresa) desde la base de datos"""
    try:
        conn = conectar_sql()
        if not conn:
            return []
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Grupo FROM [Recepcion_Consolidada] WHERE Grupo IS NOT NULL")
        grupos = sorted(set(row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()))
        conn.close()
        logger.info(f"Cargados {len(grupos)} grupos conocidos")
        return grupos
    except Exception as e:
        logger.error(f"Error cargando grupos conocidos: {str(e)}")
        return []

GRUPOS_CONOCIDOS = cargar_grupos_conocidos()

def obtener_temporada_actual():
    """Devuelve el número de temporada vigente (la de la fecha de hoy)"""
    try:
        conn = conectar_sql()
        if not conn:
            return None
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE()")
        fila = cursor.fetchone()
        conn.close()
        return int(fila[0]) if fila and fila[0] is not None else None
    except Exception as e:
        logger.error(f"Error obteniendo temporada actual: {str(e)}")
        return None

TEMPORADA_ACTUAL = obtener_temporada_actual()

def normalizar_variedad(variedad_usuario):
    """Limpia espacios; la traducción al nombre exacto ya la hace Claude usando VARIEDADES_CONOCIDAS"""
    return variedad_usuario.strip()

TOOLS = [
    {
        "name": "consultar_bins_estimados",
        "description": "Consulta cuántos bins/kg se estiman cosechar para una variedad de fruta, en una temporada. Por defecto usa la temporada vigente y la Estimación Primavera. También sirve para preguntar por una temporada distinta a la vigente (ej. la próxima temporada, que puede ya tener Estim Invierno y/o Estim Primavera cargadas antes de que empiece la cosecha real).",
        "input_schema": {
            "type": "object",
            "properties": {
                "variedad": {
                    "type": "string",
                    "description": "Nombre de la variedad mencionada por el usuario, tal como la escribió (ej. 'tiffany', 'crimson')."
                },
                "fuente_estimado": {
                    "type": "string",
                    "enum": ["trisemanal", "invierno"],
                    "description": "Omitir SIEMPRE por defecto (se usa Estim Primavera automáticamente). Solo pasar 'trisemanal' o 'invierno' si el usuario pide explícitamente esa fuente por su nombre."
                },
                "temporada": {
                    "type": "integer",
                    "description": "Número de temporada (ej. 2027 para 'la próxima temporada'), calculado usando la temporada vigente que se te indica más abajo. Omitir si el usuario pregunta por la temporada actual (por defecto)."
                }
            },
            "required": ["variedad"]
        }
    },
    {
        "name": "consultar_cosecha_hoy",
        "description": "Consulta cuántos kg se han cosechado REALMENTE (no estimado) para una variedad, en una fecha específica.",
        "input_schema": {
            "type": "object",
            "properties": {
                "variedad": {
                    "type": "string",
                    "description": "Nombre de la variedad mencionada por el usuario."
                },
                "fecha": {
                    "type": "string",
                    "description": "Fecha en formato YYYY-MM-DD, calculada a partir de la fecha de hoy y lo que diga el usuario (ej. 'ayer', 'el lunes'). Si el usuario no menciona fecha, omite este campo (se usa hoy por defecto)."
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
    {
        "name": "comparar_estimado_vs_cosechado",
        "description": "Compara lo cosechado REAL contra lo estimado (Estim Primavera por defecto) de una variedad, en una fecha específica, con porcentaje de avance. Usar cuando pregunten 'cómo vamos' de una variedad en una fecha dada (hoy, ayer, mañana, una fecha puntual, etc). Si hay estimado y real, muestra ambos.",
        "input_schema": {
            "type": "object",
            "properties": {
                "variedad": {
                    "type": "string",
                    "description": "Nombre de la variedad mencionada por el usuario."
                },
                "fecha": {
                    "type": "string",
                    "description": "Fecha en formato YYYY-MM-DD, calculada a partir de la fecha de hoy y lo que diga el usuario (ej. 'ayer', 'mañana', 'el 12 de agosto'). Si el usuario no menciona fecha, omite este campo (se usa hoy por defecto)."
                },
                "fuente_estimado": {
                    "type": "string",
                    "enum": ["trisemanal", "invierno"],
                    "description": "Omitir SIEMPRE por defecto (se usa Estim Primavera automáticamente). Solo pasar 'trisemanal' o 'invierno' si el usuario pide explícitamente esa fuente por su nombre."
                }
            },
            "required": ["variedad"]
        }
    },
    {
        "name": "consultar_resumen_productor",
        "description": "Consulta el resumen de bultos cosechados por un productor específico (no por variedad).",
        "input_schema": {
            "type": "object",
            "properties": {
                "productor": {
                    "type": "string",
                    "description": "Nombre o parte del nombre del productor mencionado por el usuario."
                }
            },
            "required": ["productor"]
        }
    },
    {
        "name": "consultar_resumen_packing",
        "description": "Consulta el resumen de bultos recibidos por una planta/packing específica (no por variedad ni productor).",
        "input_schema": {
            "type": "object",
            "properties": {
                "packing": {
                    "type": "string",
                    "description": "Nombre o parte del nombre del packing mencionado por el usuario."
                }
            },
            "required": ["packing"]
        }
    },
    {
        "name": "consultar_ultima_cosecha",
        "description": "Encuentra cuándo fue la última fecha con cosecha REAL registrada de una especie, variedad, packing/planta y/o productor, y muestra el detalle de esa fecha (variedades, productores, totales). Usar para preguntas como '¿cuándo fue la última cosecha de mandarinas?' o '¿cuándo fue la última recepción de cerezas en Lisonjera?'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "especie": {
                    "type": "string",
                    "description": "Especie mencionada por el usuario, traducida al nombre EXACTO en inglés de la lista de especies conocidas (ej. 'mandarina' -> 'MANDARIN'). Opcional."
                },
                "variedad": {
                    "type": "string",
                    "description": "Variedad específica mencionada por el usuario. Opcional."
                },
                "packing": {
                    "type": "string",
                    "description": "Planta o packing mencionado por el usuario, traducido al nombre EXACTO de la lista de packings conocidos. Opcional."
                },
                "productor": {
                    "type": "string",
                    "description": "Productor mencionado por el usuario, traducido al nombre EXACTO de la lista de productores conocidos. Opcional."
                },
                "temporada": {
                    "type": "integer",
                    "description": "Número de temporada (ej. 2025 para 'la temporada pasada'), calculado usando la temporada vigente que se te indica más abajo. Omitir si el usuario no menciona una temporada distinta a la actual."
                }
            }
        }
    },
    {
        "name": "consultar_primera_cosecha",
        "description": "Encuentra cuándo fue la PRIMERA fecha con cosecha REAL registrada de una especie, variedad, packing/planta y/o productor (cuándo empezó/se inició la cosecha), y muestra el detalle de esa fecha. Usar para preguntas como '¿cuándo empezó la cosecha de mandarinas?', '¿cuándo se inició la cosecha esta temporada/temporada pasada?'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "especie": {
                    "type": "string",
                    "description": "Especie mencionada por el usuario, traducida al nombre EXACTO en inglés de la lista de especies conocidas. Opcional."
                },
                "variedad": {
                    "type": "string",
                    "description": "Variedad específica mencionada por el usuario. Opcional."
                },
                "packing": {
                    "type": "string",
                    "description": "Planta o packing mencionado por el usuario, traducido al nombre EXACTO de la lista de packings conocidos. Opcional."
                },
                "productor": {
                    "type": "string",
                    "description": "Productor mencionado por el usuario, traducido al nombre EXACTO de la lista de productores conocidos. Opcional."
                },
                "temporada": {
                    "type": "integer",
                    "description": "Número de temporada (ej. 2025 para 'la temporada pasada'), calculado usando la temporada vigente que se te indica más abajo. Omitir si el usuario pregunta por la temporada actual (por defecto)."
                }
            }
        }
    },
    {
        "name": "consultar_cosecha_detalle",
        "description": "Consulta el detalle de cosecha (estimado y real), agrupado por packing/productor/variedad, con desglose por fecha si el rango no es muy amplio. Por defecto el estimado usa Estim Primavera. Usar para preguntas con un periodo ACOTADO y explícito, como '¿qué se cosechó ayer?', '¿cuánto se cosechó entre el 1 y el 15 de agosto?', 'kilos recepcionados en tal planta esta semana', o cuando el usuario pidió explícitamente el 'detalle'/desglose por fecha, opcionalmente filtrado por especie, variedad, productor, packing/planta o grupo. NO uses esta herramienta como respuesta por defecto a una pregunta abierta tipo 'cosecha de esta temporada' o 'cosecha de tal variedad' sin periodo acotado ni estructura pedida — en esos casos primero hay que preguntarle al usuario cómo quiere el resumen (ver instrucciones generales).",
        "input_schema": {
            "type": "object",
            "properties": {
                "fecha_inicio": {
                    "type": "string",
                    "description": "Fecha de inicio en formato YYYY-MM-DD, calculada a partir de la fecha de hoy y lo que diga el usuario. Omitir por completo si el usuario no menciona ninguna fecha o rango (se usará toda la temporada vigente)."
                },
                "fecha_fin": {
                    "type": "string",
                    "description": "Fecha de fin en formato YYYY-MM-DD. Si el usuario pregunta por un solo día, omite este campo."
                },
                "especie": {
                    "type": "string",
                    "description": "Especie mencionada por el usuario, traducida al nombre EXACTO en inglés de la lista de especies conocidas. Opcional."
                },
                "variedad": {
                    "type": "string",
                    "description": "Variedad mencionada por el usuario. Opcional."
                },
                "productor": {
                    "type": "string",
                    "description": "Productor o fundo mencionado por el usuario. Opcional."
                },
                "packing": {
                    "type": "string",
                    "description": "Planta o packing mencionado por el usuario (ej. 'recepcionado en Almahue'), traducido al nombre EXACTO de la lista de packings conocidos. 'Recepcionado' o 'recibido' en una planta/packing se refiere a esto. Opcional."
                },
                "grupo": {
                    "type": "string",
                    "description": "Grupo/holding empresarial mencionado por el usuario, traducido al nombre EXACTO de la lista de grupos conocidos. No confundir con productor: un grupo puede agrupar varios productores. Opcional."
                },
                "detalle_por_fecha": {
                    "type": "boolean",
                    "description": "Poner en true SIEMPRE que el usuario use la palabra 'detalle' (o pida explícitamente el desglose por fecha), aunque el rango de fechas sea amplio. Fuerza a mostrar la tabla con una fila por cada fecha en vez de solo totales. Omitir o dejar en false si no se pidió detalle explícitamente."
                },
                "envase": {
                    "type": "string",
                    "description": "SOLO si el usuario pregunta específicamente por bins, totes, cajas u otro tipo de envase/contenedor (no si pregunta por kilos). Traducir al nombre EXACTO de la lista de envases conocidos (ej. 'bins' -> 'BINS'). Cuando se da, la respuesta muestra la cantidad real de unidades de ese envase en vez de kilos (no se convierte desde kilos, es la cantidad real registrada). Omitir si el usuario pregunta por kilos/kg."
                },
                "temporada": {
                    "type": "integer",
                    "description": "Número de temporada (ej. 2025 para 'la temporada pasada'), calculado usando la temporada vigente que se te indica más abajo. Solo úsalo si NO se dieron fecha_inicio/fecha_fin y el usuario pidió una temporada distinta a la actual (ej. 'toda la temporada pasada'). Si el usuario da fechas explícitas, omite este campo."
                },
                "fuente_estimado": {
                    "type": "string",
                    "enum": ["trisemanal", "invierno"],
                    "description": "Omitir SIEMPRE por defecto (se usa Estim Primavera automáticamente). Solo pasar 'trisemanal' o 'invierno' si el usuario pide explícitamente esa fuente por su nombre."
                }
            }
        }
    },
    {
        "name": "consultar_comparativo_estimaciones",
        "description": "Compara Estimación Invierno vs Estimación Primavera vs Cosecha Real (con diferencias y %), agrupado por variedad. Usar para preguntas tipo 'comparativo de estimación invierno, primavera y real', 'diferencia entre lo estimado en invierno y primavera', etc. Por defecto usa toda la temporada vigente completa si no se dan fechas.",
        "input_schema": {
            "type": "object",
            "properties": {
                "especie": {
                    "type": "string",
                    "description": "Especie mencionada por el usuario, traducida al nombre EXACTO en inglés de la lista de especies conocidas. Opcional."
                },
                "variedad": {
                    "type": "string",
                    "description": "Variedad mencionada por el usuario. Opcional."
                },
                "productor": {
                    "type": "string",
                    "description": "Productor o fundo mencionado por el usuario. Opcional."
                },
                "packing": {
                    "type": "string",
                    "description": "Planta o packing mencionado por el usuario, traducido al nombre EXACTO de la lista de packings conocidos. Opcional."
                },
                "fecha_inicio": {
                    "type": "string",
                    "description": "Fecha de inicio en formato YYYY-MM-DD. Omitir si el usuario pide 'toda la temporada' o no menciona fechas (se usa la temporada completa)."
                },
                "fecha_fin": {
                    "type": "string",
                    "description": "Fecha de fin en formato YYYY-MM-DD. Omitir junto con fecha_inicio si no se mencionan fechas."
                },
                "envase": {
                    "type": "string",
                    "description": "SOLO si el usuario pide el comparativo en bins, totes, cajas u otro envase (no si pide kilos). Traducir al nombre EXACTO de la lista de envases conocidos. Cuando se da, compara la cantidad real de unidades de ese envase que tiene guardada cada una de las tres fuentes (Invierno, Primavera, Real), no un cálculo. Omitir si pregunta por kilos/kg."
                },
                "temporada": {
                    "type": "integer",
                    "description": "Número de temporada (ej. 2025 para 'la temporada pasada'), calculado usando la temporada vigente que se te indica más abajo. Solo úsalo si NO se dieron fecha_inicio/fecha_fin y el usuario pidió una temporada distinta a la actual."
                }
            }
        }
    },
    {
        "name": "consultar_cosecha_flexible",
        "description": "Consulta GENÉRICA de estimado y real, agrupada EXACTAMENTE por las dimensiones que pida el usuario (cualquier combinación de especie, variedad, productor, packing, grupo, fecha, semana — o ninguna si pide solo el total sin desglose). Usar cuando el usuario pide una estructura específica que no calza con las otras herramientas, por ejemplo: 'estimación de cosecha por especie' (agrupar_por=['especie']), 'informe con columnas fecha, estimado y real' (agrupar_por=['fecha']), 'total por productor' (agrupar_por=['productor']), 'solo el total' o 'cuánto es en total' (agrupar_por=[], sin desglose), 'por semana' (agrupar_por=['semana']). Responde solo con las columnas pedidas, nada más. ESTA HERRAMIENTA NO TIENE PERIODO POR DEFECTO (a diferencia de las demás): si el usuario menciona CUALQUIER periodo, aunque sea 'esta temporada' o 'hasta hoy', DEBES pasar temporada o fecha_inicio/fecha_fin explícitamente — nunca los omitas solo porque suene al comportamiento por defecto de otras herramientas. Solo omite ambos si el usuario literalmente no dijo nada sobre tiempo, para que la herramienta pida aclaración.",
        "input_schema": {
            "type": "object",
            "properties": {
                "agrupar_por": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["especie", "variedad", "productor", "packing", "grupo", "fecha", "semana"]},
                    "description": "Dimensiones exactas por las que agrupar, en el orden pedido por el usuario. Ej. 'resumen por especie' -> ['especie']; 'informe con fecha, estimado y real' -> ['fecha']; 'total por productor y variedad' -> ['productor','variedad']; 'solo el total, sin desglose' -> [] (arreglo vacío); 'por semana' -> ['semana'] (agrupa por número de semana calendario, la misma columna Semana de la base, en el orden natural de la temporada 44..52, 1..43)."
                },
                "fecha_inicio": {
                    "type": "string",
                    "description": "Fecha de inicio en formato YYYY-MM-DD. Omitir si el usuario no mencionó ningún periodo (deja que la herramienta pida aclaración) o si mencionó una temporada en vez de fechas."
                },
                "fecha_fin": {
                    "type": "string",
                    "description": "Fecha de fin en formato YYYY-MM-DD. Omitir junto con fecha_inicio."
                },
                "temporada": {
                    "type": "integer",
                    "description": "OBLIGATORIO si el usuario mencionó cualquier referencia a temporada, incluyendo 'esta temporada' (usa el número de la temporada vigente indicada más abajo) o 'la temporada pasada' (vigente menos 1)."
                },
                "especie": {
                    "type": "string",
                    "description": "Filtro opcional de especie, traducido al nombre EXACTO en inglés de la lista de especies conocidas."
                },
                "variedad": {
                    "type": "string",
                    "description": "Filtro opcional de variedad."
                },
                "productor": {
                    "type": "string",
                    "description": "Filtro opcional de productor."
                },
                "packing": {
                    "type": "string",
                    "description": "Filtro opcional de planta/packing."
                },
                "grupo": {
                    "type": "string",
                    "description": "Filtro opcional de grupo/holding empresarial, traducido al nombre EXACTO de la lista de grupos conocidos (ej. 'Valdés', 'Rodríguez', 'Superfruit'). No confundir con productor: un grupo puede agrupar varios productores."
                },
                "envase": {
                    "type": "string",
                    "description": "SOLO si el usuario pide el resultado en bins, totes, cajas u otro envase (no kilos). Nombre EXACTO de la lista de envases conocidos."
                },
                "fuente_estimado": {
                    "type": "string",
                    "enum": ["trisemanal", "invierno"],
                    "description": "Omitir SIEMPRE por defecto (se usa Estim Primavera automáticamente). Solo pasar 'trisemanal' o 'invierno' si el usuario pide explícitamente esa fuente por su nombre."
                },
                "kg_por_caja_eq": {
                    "type": "number",
                    "description": "Cuántos kilos equivalen a una Caja Eq para la variedad consultada. Pasarlo SOLO cuando el usuario ya respondió esa cifra, después de que la herramienta la haya pedido. No inventarlo ni asumir un valor típico."
                }
            },
            "required": ["agrupar_por"]
        }
    },
]

# Esta herramienta está SIEMPRE disponible (ver procesar_mensaje): el usuario puede reportar
# un ajuste de proyección cuando ocurre, no solo mientras haya un cuestionario vigente. Si hay
# cuestionario vigente actualiza esa respuesta; si no, queda como reporte espontáneo.
TOOL_REGISTRAR_CUESTIONARIO = {
    "name": "registrar_respuesta_cuestionario",
    "description": "Registra que el usuario REPORTA un cambio en su proyección de cosecha, para avisar al equipo EAS. Usar SIEMPRE que el usuario informe (no que pregunte) que va a cosechar más o menos de lo proyectado, que algo viene atrasado o adelantado, o cualquier novedad de campo que afecte lo esperado: 'la boreal viene con 2.000 kilos menos', 'serán unos 10.000 kg menos de tiffany', 'la garcica se atrasó una semana', 'es solo una corrección para avisar a los EAS'. También registra la respuesta al cuestionario de proyección cuando hay uno vigente ('confirmo', 'ok') y corrige una respuesta anterior ('mejor serán 10 mil más'), reemplazando por completo lo registrado antes. NO usar cuando el usuario PREGUNTA por datos ('cuánto se cosechó de boreal', 'cuál es el estimado') — eso son consultas, no reportes.",
    "input_schema": {
        "type": "object",
        "properties": {
            "resultado": {
                "type": "string",
                "enum": ["confirmado", "ajustado"],
                "description": "'ajustado' siempre que el usuario reporte cualquier cambio o corrección respecto de lo proyectado. 'confirmado' SOLO cuando responde a un cuestionario vigente diciendo que la proyección está bien tal cual."
            },
            "detalle_ajuste": {
                "type": "string",
                "description": "Obligatorio si resultado='ajustado': resumen claro y concreto de lo que reportó el usuario, con variedad, magnitud y unidad si las dio. Ej: 'BOREAL: ~2.000 kg menos que lo proyectado'. Si no dio cifras, describe el cambio igual: 'GARCICA: cosecha atrasada aprox. una semana'."
            }
        },
        "required": ["resultado"]
    },
}

def construir_system_prompt(es_audio=False, cuestionario_activo=None, alcance=None):
    if VARIEDADES_CONOCIDAS:
        lista_variedades = ", ".join(VARIEDADES_CONOCIDAS)
    else:
        lista_variedades = "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"

    lista_especies = ", ".join(f"{en} ({es})" for en, es in ESPECIE_TRADUCCION.items())

    if PACKINGS_CONOCIDOS:
        lista_packings = ", ".join(PACKINGS_CONOCIDOS)
    else:
        lista_packings = "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"

    alias_packing_texto = "; ".join(
        f'{real} = {" / ".join(alias)}' for real, alias in ALIAS_PACKING.items()
    )

    if ENVASES_CONOCIDOS:
        lista_envases = ", ".join(ENVASES_CONOCIDOS)
    else:
        lista_envases = "(lista no disponible por ahora)"

    if PRODUCTORES_CONOCIDOS:
        lista_productores = ", ".join(PRODUCTORES_CONOCIDOS)
    else:
        lista_productores = "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"

    if GRUPOS_CONOCIDOS:
        lista_grupos = ", ".join(GRUPOS_CONOCIDOS)
    else:
        lista_grupos = "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"

    temporada_texto = (
        f"La temporada vigente (actual) es {TEMPORADA_ACTUAL}."
        if TEMPORADA_ACTUAL else "(no se pudo determinar la temporada vigente)"
    )

    hoy = datetime.now().strftime("%Y-%m-%d (%A)")

    nota_audio = ""
    if es_audio:
        nota_audio = """
ATENCIÓN: este mensaje viene de una transcripción automática de una nota de voz, puede tener errores
FONÉTICOS (palabras que suenan parecido pero se transcribieron distinto, ej. "beans" en vez de "bins",
"crimen" en vez de "crimson", nombres de productores o plantas mal transcritos). Ten esto muy en cuenta
al interpretar el mensaje: prioriza qué palabra conocida SUENA parecido a lo transcrito, no solo cuál se
escribe parecido.
"""

    nota_cuestionario = ""
    if cuestionario_activo and cuestionario_activo.get("estado") == "pendiente":
        nota_cuestionario = f"""
CUESTIONARIO PENDIENTE: hace poco el bot le envió a este usuario el siguiente cuestionario de
proyección de cosecha y todavía no lo responde:
---
{cuestionario_activo["mensaje"]}
---
Si el mensaje del usuario es una respuesta a ese cuestionario (confirma la proyección, o indica un
ajuste o corrección de cifras, aunque sea informal: "ok", "confirmo", "está bien", "va a ser menos",
"serán unos 10 mil kilos menos de tiffany"), usa la herramienta registrar_respuesta_cuestionario
con resultado "confirmado" o "ajustado" y el detalle del ajuste si lo hay. Si el mensaje es una
consulta normal que no tiene relación con el cuestionario, atiéndela con las demás herramientas
como siempre (el cuestionario queda pendiente; no insistas con él en cada mensaje).
"""
    elif cuestionario_activo:
        respuesta_previa = (
            cuestionario_activo.get("ajuste")
            or cuestionario_activo.get("respuesta")
            or "(confirmó la proyección tal cual)"
        )
        nota_cuestionario = f"""
CUESTIONARIO YA RESPONDIDO (todavía corregible): hace poco el bot le envió a este usuario el
siguiente cuestionario de proyección de cosecha:
---
{cuestionario_activo["mensaje"]}
---
El usuario ya lo respondió (estado: {cuestionario_activo["estado"]}; lo registrado: {respuesta_previa}).
Si su mensaje de ahora es una CORRECCIÓN o un nuevo ajuste sobre esa misma proyección (ej. "mejor
serán 10 mil kilos más", "me equivoqué, era garcica", "al final déjalo como estaba"), usa la
herramienta registrar_respuesta_cuestionario para ACTUALIZAR lo registrado: resultado "ajustado"
con el detalle nuevo COMPLETO (reemplaza al anterior, no lo complementa), o "confirmado" si vuelve
a la proyección original. Si el mensaje es una consulta normal que no tiene relación con el
cuestionario, atiéndela con las demás herramientas como siempre.
"""
    else:
        nota_cuestionario = """
REPORTES DE AJUSTE: este usuario no tiene ningún cuestionario de proyección vigente ahora, pero
IGUAL puede reportar en cualquier momento que su cosecha va a venir distinta de lo proyectado
(ej. "la boreal viene con 2.000 kilos menos", "la garcica se atrasó una semana", "es solo una
corrección para avisar a los EAS"). Cuando el usuario INFORME un cambio así —no cuando pregunte
por datos— usa la herramienta registrar_respuesta_cuestionario con resultado "ajustado" y el
detalle concreto: queda registrado y se le avisa al equipo EAS. NUNCA le digas que no puedes
registrar ajustes ni que debe avisarle al EAS por otro canal: sí puedes, con esa herramienta.
"""

    nota_alcance = ""
    if alcance:
        nota_alcance = f"""
ALCANCE DE ESTE USUARIO: solo tiene acceso a la información de estos fundos/productores:
{", ".join(alcance)}.
Las herramientas ya filtran automáticamente por esos fundos, así que los resultados que
recibas SIEMPRE corresponden solo a ellos: no necesitas agregar filtros ni advertir nada en
cada respuesta. Si el usuario pregunta por un productor o fundo que NO está en esa lista,
explícale con naturalidad que solo puedes darle información de los fundos que tiene
asignados, y nómbraselos. Nunca inventes ni estimes datos de fundos fuera de su alcance.
"""

    return f"""Eres el asistente de WhatsApp de Agua Santa para consultas de cosecha de fruta.
{nota_audio}{nota_cuestionario}{nota_alcance}

Hoy es {hoy}. Usa esta fecha como referencia para calcular fechas relativas que mencione el usuario
("ayer", "hoy", "mañana", "el lunes pasado", "el 12 de agosto", "entre el 1 y el 15 de agosto", etc.)
y pásalas a las herramientas en formato YYYY-MM-DD. SIEMPRE calcula fechas concretas, nunca pases texto
literal como "hoy" o "ayer" a una herramienta. Ejemplos: "esta semana" = desde el lunes de esta semana
hasta hoy; "la semana pasada" = lunes a domingo de la semana anterior; "hasta hoy" = fecha_inicio que
corresponda y fecha_fin = hoy. Si el usuario no menciona ninguna fecha (ej. "toda la temporada", "cuánto
llevamos cosechado"), omite fecha_inicio y fecha_fin por completo en vez de inventar una fecha.

{temporada_texto} Las temporadas se numeran como años (ej. 2026, 2025, ...). Si el usuario pregunta por
"esta temporada"/"esta año" no hace falta nada especial (es el comportamiento por defecto). Si pregunta
por "la temporada pasada"/"el año pasado" usa el parámetro "temporada" con el número de la temporada
vigente menos 1; "hace 2 temporadas" sería menos 2, y así sucesivamente. Usa "temporada" (no fechas)
salvo que el usuario también dé fechas específicas dentro de esa temporada.

También puede preguntar por una temporada FUTURA: "la próxima temporada"/"la temporada que viene" es
la vigente más 1, y así sucesivamente ("en 2 temporadas más" sería más 2). Esto es válido aunque esa
temporada todavía no empiece ni tenga cosecha real: puede tener ya cargada Estim Invierno y/o Estim
Primavera (la planificación de una temporada suele empezar antes de que termine la anterior), así que
no asumas que "no ha empezado" significa que no hay nada que consultar — intenta la consulta igual.

EXCEPCIÓN a lo anterior: consultar_cosecha_flexible NO tiene ningún periodo por defecto. Si vas a llamar
esa herramienta específicamente y el usuario dijo "esta temporada" (o cualquier referencia temporal),
DEBES pasar temporada={TEMPORADA_ACTUAL} explícitamente (o las fechas que correspondan) — no lo omitas
pensando que hay un default, porque en esa herramienta omitirlo significa "el usuario no dijo nada" y
hará que se le pregunte innecesariamente.

Tienes herramientas para consultar, por variedad: estimado de temporada, cosecha real en
una fecha, y comparación de avance (estimado vs cosechado real) en una fecha.
También puedes consultar resúmenes por productor o por packing (no requieren variedad), cuándo fue la
primera o la última cosecha real de una especie/variedad/productor/packing (útil para "¿cuándo empezó
la cosecha?" o "¿cuándo fue la última?"), y el detalle de cosecha real entre un rango de fechas
(agrupado por especie/variedad/productor, opcionalmente filtrado por especie, variedad o productor).

MUY IMPORTANTE — FUENTE DEL "ESTIMADO": hay tres fuentes distintas de estimado en la base: Trisemanal
(rolling 3 semanas), Estim Invierno y Estim Primavera (dos ciclos de planificación de temporada). Por
defecto, SIEMPRE que hables de "estimado" o "estimación" (sin que el usuario nombre una fuente distinta),
usa Estim Primavera — no hace falta pasar ningún parámetro especial, es el comportamiento por defecto de
consultar_bins_estimados, comparar_estimado_vs_cosechado, consultar_cosecha_detalle y
consultar_cosecha_flexible. Solo si el usuario menciona explícitamente "trisemanal" o "estimación
invierno"/"estimado invierno" (una sola fuente, no una comparación), pasa fuente_estimado="trisemanal" o
fuente_estimado="invierno" en esa misma herramienta para usar esa fuente en vez de Primavera.

Además tienes consultar_comparativo_estimaciones: compara Estimación Invierno vs Estimación Primavera
vs Cosecha Real EN LA MISMA TABLA, con diferencias y %. Úsala específicamente cuando el usuario pida
COMPARAR ambas estimaciones (ej. "comparativo de estimación invierno, primavera y real", "diferencia
entre lo estimado en invierno y primavera", "cómo cambió la estimación de invierno a primavera") — no
para una consulta simple de una sola fuente, que ya cubren las herramientas normales con
fuente_estimado.

También tienes consultar_cosecha_flexible: para cuando el usuario pide una estructura o agrupación
específica que no calza con las demás herramientas (ej. "estimación de cosecha por especie", "informe
con columnas fecha, estimado y real", "total por productor", "por grupo", "por semana", o "solo el
total sin desglose" con agrupar_por=[]). Responde SOLO con lo que se pidió, ni más ni menos — si piden
agrupar solo por especie, no agregues variedad/productor/fecha aunque los tengas disponibles.
"Por semana" (agrupar_por=['semana']) agrupa por la semana calendario ISO estándar (la misma columna
"Semana" que usa la base de datos, 1-52, lunes a domingo) en el orden natural de la temporada
(44, 45, ..., 52, 1, 2, ..., 43) — la tabla muestra solo el número de semana; no necesitas
calcular ni agregar nada.

Usa la herramienta que corresponda cuando el usuario pregunte por alguno de esos datos y haya mencionado
(o puedas inferir) el dato que falta (variedad, especie, productor, packing, fecha o rango de fechas).

MUY IMPORTANTE: cuando el usuario pida un dato (cosecha, estimado, comparativo, etc.), SIEMPRE debes
llamar a la herramienta correspondiente para obtener el dato ACTUAL, incluso si en la conversación
anterior ya respondiste algo parecido o idéntico. NUNCA repitas, parafrasees ni reutilices un resultado
de una respuesta anterior sin volver a ejecutar la herramienta — los datos pueden haber cambiado o
haberse corregido, y responder desde memoria puede dar información desactualizada o incorrecta.

MUY IMPORTANTE — PREGUNTA LA ESTRUCTURA ANTES DE ASUMIR: cuando el usuario pida la cosecha/estimado/real
de algo (una especie, variedad, productor, packing, grupo, o una temporada) SIN indicar cómo quiere que
se resuma la respuesta —es decir, no dijo "detalle", no pidió agrupar "por especie/productor/packing/
grupo", y no especificó columnas o una estructura concreta— Y ADEMÁS el periodo es abierto (no dio una
fecha o rango acotado como "ayer", "hoy", "esta semana", "entre el X y el Y", sino que el resultado
abarcaría toda la temporada o un periodo sin acotar), NO llames a ninguna herramienta todavía. En vez de
eso, pregúntale primero cómo quiere el resumen, por ejemplo: "¿Cómo quieres que te lo resuma? Puedo
darte el total general, o desglosado por productor/fundo, por especie, por packing/planta, por grupo,
por semana, o el detalle completo día por día." Si tampoco quedó claro el periodo (recuerda que consultar_cosecha_flexible,
a diferencia de las demás, NO tiene periodo por defecto), agrega esa pregunta AL MISMO TIEMPO, en el
mismo mensaje, para no tener que preguntar dos veces seguidas — ej. "...y ¿para qué periodo? (esta
temporada, un rango de fechas, etc.)". Esto aplica también si el usuario responde a esta pregunta
indicando SOLO la agrupación (ej. "por productor") sin mencionar el periodo: antes de llamar a la
herramienta, pregunta el periodo que falte en vez de asumirlo. Solo después de tener ambos datos, llama
a la herramienta que corresponda: consultar_cosecha_flexible con el agrupar_por elegido (agrupar_por=[]
si pide solo el total, sin desglose) y el periodo indicado, o consultar_cosecha_detalle con
detalle_por_fecha=true si pide el detalle completo por fecha. Ejemplos que DEBEN generar esta pregunta
antes de consultar: "cosecha de esta temporada", "estimación de cosecha por especie" (sin decir período
ni tenerlo de una respuesta previa), "cosecha de santina", "dame la cosecha del grupo Valdés". Si en
cambio el usuario SÍ
dio una fecha o rango acotado (ej. "¿qué se cosechó ayer?", "entre el 1 y el 15 de agosto", "esta
semana"), no hace falta preguntar nada: usa consultar_cosecha_detalle directamente, como siempre.

Si el usuario usa la palabra "detalle" (ej. "dame el detalle de...", "detalle de cosecha de..."), SIEMPRE
llama a consultar_cosecha_detalle con detalle_por_fecha=true, aunque pregunte solo por la cosecha en
general y no mencione fechas explícitamente — igual debe mostrarse el desglose por fecha. Usar la
palabra "detalle" ya cuenta como estructura indicada, así que en ese caso NO hace falta preguntar nada
más (salvo que falte el periodo).

VARIEDADES CONOCIDAS EN EL SISTEMA (nombre exacto como está en la base de datos):
{lista_variedades}

Cuando el usuario mencione una variedad, identifica a cuál de esta lista se refiere aunque la escriba
distinto (sin tildes, con errores de tipeo, abreviada, en otro idioma, etc. — ej. "tiffany" es "TIFANY",
"murcott" es "W. MURCOTT") y pasa a la herramienta el nombre EXACTO tal como aparece en esta lista.
Si no reconoces ninguna variedad de la lista que calce razonablemente, pídele al usuario que aclare
en vez de adivinar.

ESPECIES CONOCIDAS EN EL SISTEMA (nombre real en inglés y su traducción):
{lista_especies}

Cuando el usuario mencione una especie (en español, plural, singular, etc.), pasa a la herramienta el
nombre EXACTO en inglés de esta lista (ej. "mandarinas" -> "MANDARIN", "uva" -> "GRAPE").

PACKINGS/PLANTAS CONOCIDOS EN EL SISTEMA (nombre exacto como está en la base de datos):
{lista_packings}

Alias conocidos para packings/plantas: {alias_packing_texto}

Cuando el usuario mencione una planta o packing (con su nombre completo o un alias, ej. "recepcionado
en Almahue", "recibido en Santa Ana"), pasa a la herramienta el nombre EXACTO de la lista de packings.
Las palabras "recepcionado" o "recibido" en una planta/packing significan lo mismo que "cosechado real"
pero filtrado por esa planta.

PRODUCTORES CONOCIDOS EN EL SISTEMA (nombre exacto como está en la base de datos):
{lista_productores}

Cuando el usuario mencione un productor, pasa a la herramienta el nombre EXACTO de esta lista.

GRUPOS (HOLDINGS EMPRESARIALES) CONOCIDOS EN EL SISTEMA (nombre exacto como está en la base de datos):
{lista_grupos}

Un "grupo" es la empresa/holding dueña de uno o varios productores (ej. el grupo "VALDES" agrupa varios
fundos). No es lo mismo que un productor ni que una planta/packing. Cuando el usuario mencione un grupo
por su nombre, o pida agrupar/filtrar "por grupo", usa el parámetro "grupo" con el nombre EXACTO de esta
lista.

ATENCIÓN — AMBIGÜEDAD PRODUCTOR VS PLANTA/PACKING: varios nombres existen TANTO en la lista de
productores COMO en la de plantas/packings (ej. "El Carmelo" es un productor Y también una planta;
lo mismo puede pasar con "Lisonjera", "Almahue", "Santa Ana", "La Higuera", etc.). Si el usuario
menciona uno de estos nombres y el mensaje NO deja claro si se refiere al productor (de dónde viene
la fruta) o a la planta/packing (dónde se procesa), NO asumas ni elijas uno por tu cuenta: pregúntale
directamente al usuario cuál de los dos quiso decir antes de llamar a ninguna herramienta. Si el
contexto de la conversación ya lo aclaró antes, no vuelvas a preguntar.

TIPOS DE ENVASE CONOCIDOS EN EL SISTEMA (nombre exacto como está en la base de datos):
{lista_envases}

Si el usuario pregunta específicamente por bins, totes, cajas u otro contenedor físico (no por kilos),
usa el parámetro "envase" de consultar_cosecha_detalle con el nombre EXACTO de esta lista. Esto muestra
la cantidad REAL de unidades registradas de ese envase, no una conversión calculada desde kilos (los
kilos por unidad varían según la fruta y no son un factor fijo confiable). Si el usuario pregunta por
kilos/kg, no uses este parámetro.

CAJAS EQ (caso especial): "Caja Eq" es una unidad que solo algunas especies/variedades manejan en el
sistema (principalmente uva). Cuando el usuario pregunte por cajas eq / cajas equivalentes, usa
SIEMPRE consultar_cosecha_flexible con envase="CAJA EQ" (esa herramienta es la única que sabe
manejar este caso). Si la variedad sí tiene cajas eq registradas, te devolverá los datos normalmente.
Si NO las tiene, te devolverá una pregunta pidiendo cuántos kilos equivalen a una Caja Eq: trasládale
esa pregunta al usuario tal cual, sin inventar ni suponer un factor. Cuando el usuario responda con la
cifra (ej. "8,2 kilos", "son 10 kilos por caja"), vuelve a llamar a consultar_cosecha_flexible con los
MISMOS parámetros de la consulta original MÁS kg_por_caja_eq con ese número, y entrégale el resultado
ya convertido.

Si el usuario saluda, pide ayuda, o pregunta algo que no corresponde a ninguna herramienta, respóndele tú
directamente: breve, amable, en español, y si corresponde explícale qué puedes hacer.

Si falta la variedad, especie, productor o packing para poder consultar, pídeselo al usuario en vez de
inventarlo.

Si una palabra del mensaje NO calza exactamente con ninguna variedad/especie/packing/envase conocido,
NUNCA te rindas de inmediato ni respondas solo "no entendí" o listes TODAS las opciones disponibles.
En vez de eso: identifica cuáles 1 a 3 opciones de las listas conocidas se parecen MÁS (por escritura o,
si el mensaje viene de audio, por sonido) a lo que escribió/dijo el usuario, y pregúntale de forma breve
cuál de esas quiso decir (ej. "¿Te referías a BINS?" o "¿Es BINS, TOTES o CAJA EQ?"). Solo si de verdad
no hay ninguna opción remotamente parecida, ahí sí pide que aclare sin sugerir nada. El objetivo es que
el usuario nunca se quede sin poder avanzar la conversación."""

def ejecutar_tool(tool_name, tool_input, numero_sender=None, texto_usuario=None):
    # Barrera de alcance: zonal y productor solo ven sus fundos. Se calcula aquí, desde el
    # número que escribe, y NO desde nada que venga en el mensaje o que decida el modelo.
    alcance = alcance_de(numero_sender) if numero_sender else None
    if alcance is not None and not alcance:
        return MENSAJE_SIN_FUNDOS

    if tool_name == "registrar_respuesta_cuestionario":
        return registrar_respuesta_cuestionario(
            numero_sender,
            tool_input.get("resultado", "confirmado"),
            detalle_ajuste=tool_input.get("detalle_ajuste"),
            texto_usuario=texto_usuario,
        )

    if tool_name == "consultar_resumen_productor":
        return obtener_resumen_por_productor(tool_input.get("productor", ""), alcance=alcance)
    if tool_name == "consultar_resumen_packing":
        return obtener_resumen_por_packing(tool_input.get("packing", ""), alcance=alcance)

    if tool_name == "consultar_ultima_cosecha":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_ultima_cosecha(
            especie=especie,
            variedad=variedad_op,
            packing=tool_input.get("packing") or None,
            productor=tool_input.get("productor") or None,
            temporada=tool_input.get("temporada") or None,
            alcance=alcance,
        )

    if tool_name == "consultar_primera_cosecha":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_primera_cosecha(
            especie=especie,
            variedad=variedad_op,
            packing=tool_input.get("packing") or None,
            productor=tool_input.get("productor") or None,
            temporada=tool_input.get("temporada") or None,
            alcance=alcance,
        )

    if tool_name == "consultar_cosecha_detalle":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_cosecha_detalle(
            fecha_inicio=tool_input.get("fecha_inicio"),
            fecha_fin=tool_input.get("fecha_fin") or None,
            especie=especie,
            variedad=variedad_op,
            productor=tool_input.get("productor") or None,
            packing=tool_input.get("packing") or None,
            grupo=tool_input.get("grupo") or None,
            forzar_fechas=bool(tool_input.get("detalle_por_fecha")),
            envase=tool_input.get("envase") or None,
            temporada=tool_input.get("temporada") or None,
            fuente_estimado=tool_input.get("fuente_estimado") or None,
            alcance=alcance,
        )

    if tool_name == "consultar_comparativo_estimaciones":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_comparativo_estimaciones(
            especie=especie,
            variedad=variedad_op,
            productor=tool_input.get("productor") or None,
            packing=tool_input.get("packing") or None,
            fecha_inicio=tool_input.get("fecha_inicio") or None,
            fecha_fin=tool_input.get("fecha_fin") or None,
            envase=tool_input.get("envase") or None,
            temporada=tool_input.get("temporada") or None,
            alcance=alcance,
        )

    if tool_name == "consultar_cosecha_flexible":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_cosecha_flexible(
            agrupar_por=tool_input.get("agrupar_por") or [],
            fecha_inicio=tool_input.get("fecha_inicio") or None,
            fecha_fin=tool_input.get("fecha_fin") or None,
            especie=especie,
            variedad=variedad_op,
            productor=tool_input.get("productor") or None,
            packing=tool_input.get("packing") or None,
            grupo=tool_input.get("grupo") or None,
            envase=tool_input.get("envase") or None,
            temporada=tool_input.get("temporada") or None,
            fuente_estimado=tool_input.get("fuente_estimado") or None,
            kg_por_caja_eq=tool_input.get("kg_por_caja_eq") or None,
            alcance=alcance,
        )

    variedad = normalizar_variedad(tool_input.get("variedad", ""))
    fecha = tool_input.get("fecha") or None
    fuente_estimado = tool_input.get("fuente_estimado") or None
    if tool_name == "consultar_bins_estimados":
        return obtener_bins_estimados(variedad, fuente_estimado=fuente_estimado, temporada=tool_input.get("temporada") or None, alcance=alcance)
    elif tool_name == "consultar_cosecha_hoy":
        return obtener_cosecha_actual(variedad, fecha, alcance=alcance)
    elif tool_name == "consultar_calibre_promedio":
        return obtener_calibre_promedio(variedad)
    elif tool_name == "comparar_estimado_vs_cosechado":
        return obtener_comparacion_estimado_vs_cosechado(variedad, fecha, fuente_estimado=fuente_estimado, alcance=alcance)
    return "No supe qué información buscar para esa pregunta."

def procesar_mensaje(texto_mensaje, numero_sender=None, es_audio=False):
    """
    Usa Claude para interpretar el mensaje: decide si llamar una herramienta de consulta
    o responder directamente (saludo, ayuda, pregunta fuera de alcance).
    Si se da numero_sender, incluye los mensajes recientes de esa conversación como
    contexto, para que Claude entienda preguntas de seguimiento.
    Si es_audio=True, el mensaje viene de una transcripción de voz y puede tener errores
    fonéticos (ej. "beans" en vez de "bins").
    """
    try:
        messages = []
        if numero_sender:
            for turno in obtener_historial_conversacion(numero_sender):
                messages.append({"role": "user", "content": turno["mensaje"]})
                messages.append({"role": "assistant", "content": turno["respuesta"]})
        messages.append({"role": "user", "content": texto_mensaje})

        cuestionario_activo = obtener_cuestionario_activo(numero_sender) if numero_sender else None
        # La herramienta de reporte va siempre: un ajuste de proyección se avisa cuando pasa,
        # no solo mientras hay un cuestionario vigente.
        tools = (TOOLS + [TOOL_REGISTRAR_CUESTIONARIO]) if numero_sender else TOOLS

        response = claude_client.messages.create(
            model="claude-sonnet-5",
            # 300 se quedaba corto: con el prompt actual (más largo) Claude a veces gasta el
            # presupuesto completo en el bloque de "thinking" antes de terminar el tool_use,
            # devolviendo una respuesta trunca (stop_reason="max_tokens") sin tool_use ni texto.
            max_tokens=1500,
            system=construir_system_prompt(
                es_audio,
                cuestionario_activo=cuestionario_activo,
                alcance=alcance_de(numero_sender) if numero_sender else None,
            ),
            tools=tools,
            messages=messages,
        )

        tool_use_block = next((b for b in response.content if b.type == "tool_use"), None)
        if tool_use_block:
            tool_input = tool_use_block.input
            # Si el usuario pidió "detalle" explícitamente, forzamos el desglose por fecha
            # sin depender de que Claude lo haya marcado (instrucción "sí o sí").
            if tool_use_block.name == "consultar_cosecha_detalle" and "detalle" in texto_mensaje.lower():
                tool_input = {**tool_input, "detalle_por_fecha": True}
            return ejecutar_tool(tool_use_block.name, tool_input, numero_sender=numero_sender, texto_usuario=texto_mensaje)

        text_block = next((b for b in response.content if b.type == "text"), None)
        if text_block:
            return text_block.text

        return "No entendí tu pregunta. Escribe 'ayuda' para ver qué puedo hacer."
    except Exception as e:
        logger.error(f"Error en procesar_mensaje (Claude): {str(e)}")
        return "Tuve un problema procesando tu mensaje. Intenta de nuevo en un momento."

# ============================================================================
# NOTAS DE VOZ
# ============================================================================

def descargar_audio_whatsapp(media_id):
    """Descarga el archivo de audio de un mensaje de WhatsApp a partir de su media_id"""
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}

    meta_resp = requests.get(f"https://graph.facebook.com/v22.0/{media_id}", headers=headers)
    meta_resp.raise_for_status()
    meta = meta_resp.json()

    audio_resp = requests.get(meta["url"], headers=headers)
    audio_resp.raise_for_status()

    return audio_resp.content, meta.get("mime_type", "audio/ogg")

def transcribir_audio(audio_bytes, mime_type="audio/ogg"):
    """Transcribe una nota de voz a texto usando OpenAI Whisper"""
    if not openai_client:
        raise RuntimeError("OPENAI_API_KEY no configurada")

    extension = "mp3" if "mp3" in mime_type or "mpeg" in mime_type else "ogg"
    audio_file = io.BytesIO(audio_bytes)
    audio_file.name = f"nota_voz.{extension}"

    transcripcion = openai_client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
        language="es",
    )
    return transcripcion.text

# ============================================================================
# ENVÍO POR WHATSAPP
# ============================================================================

LIMITE_WHATSAPP_TEXTO = 4000  # margen bajo el límite real de Meta (4096 caracteres por mensaje)

def dividir_mensaje_whatsapp(texto, limite=LIMITE_WHATSAPP_TEXTO):
    """Divide un texto largo en partes que respeten el límite de caracteres de WhatsApp,
    cortando preferentemente en saltos de párrafo para no partir tablas a la mitad."""
    if len(texto) <= limite:
        return [texto]

    bloques = texto.split("\n\n")
    partes = []
    actual = ""
    for bloque in bloques:
        candidato = f"{actual}\n\n{bloque}" if actual else bloque
        if len(candidato) > limite and actual:
            partes.append(actual)
            actual = bloque
        else:
            actual = candidato
    if actual:
        partes.append(actual)

    # Por si un solo bloque (sin saltos de párrafo) sigue superando el límite
    partes_finales = []
    for parte in partes:
        while len(parte) > limite:
            partes_finales.append(parte[:limite])
            parte = parte[limite:]
        if parte:
            partes_finales.append(parte)
    return partes_finales

def _enviar_whatsapp_una_parte(numero_destino, mensaje_texto):
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
        if response.status_code != 200:
            logger.error(f"WhatsApp respondió {response.status_code} al enviar a {numero_destino}: {response.text}")
            return False
        # Un 200 solo significa "Meta aceptó el mensaje", no que se haya entregado: el
        # resultado real llega después por los statuses del webhook.
        registrar_mensaje_enviado(response, numero_destino)
        logger.info(f"WhatsApp aceptado por Meta para {numero_destino}: {response.status_code}")
        return True
    except Exception as e:
        logger.error(f"Error enviando WhatsApp: {str(e)}")
        return False

def registrar_mensaje_enviado(response, numero_destino):
    """Guarda el id (wamid) que devuelve Meta, para poder cruzarlo con el estado de entrega."""
    try:
        mensajes = (response.json() or {}).get("messages") or []
        if not mensajes:
            return
        conn = sqlite3.connect(DB_LOCAL_PATH)
        for m in mensajes:
            conn.execute(
                "INSERT OR REPLACE INTO mensajes_estado (wamid, numero, estado) VALUES (?, ?, 'aceptado')",
                (m.get("id"), normalizar_numero(numero_destino))
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando mensaje enviado: {str(e)}")

def registrar_estado_entrega(status):
    """Procesa un 'status' del webhook: sent / delivered / read / failed, con su error."""
    try:
        wamid = status.get("id")
        if not wamid:
            return
        estado = status.get("status")
        errores = status.get("errors") or []
        error_code = str(errores[0].get("code")) if errores else None
        if errores:
            e0 = errores[0]
            detalle = e0.get("title") or e0.get("message") or ""
            extra = (e0.get("error_data") or {}).get("details")
            error_detalle = f"{detalle} — {extra}" if extra else detalle
            logger.error(f"WhatsApp NO entregado a {status.get('recipient_id')}: [{error_code}] {error_detalle}")
        else:
            error_detalle = None
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute(
            "INSERT INTO mensajes_estado (wamid, numero, estado, error_code, error_detalle, fecha_estado) "
            "VALUES (?, ?, ?, ?, ?, datetime('now','localtime')) "
            "ON CONFLICT(wamid) DO UPDATE SET estado = excluded.estado, "
            "error_code = COALESCE(excluded.error_code, mensajes_estado.error_code), "
            "error_detalle = COALESCE(excluded.error_detalle, mensajes_estado.error_detalle), "
            "fecha_estado = excluded.fecha_estado",
            (wamid, normalizar_numero(status.get("recipient_id")), estado, error_code, error_detalle)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando estado de entrega: {str(e)}")

def enviar_whatsapp(numero_destino, mensaje_texto):
    """
    Envía mensaje por WhatsApp Business API (Meta). Si el texto supera el límite de
    caracteres de un mensaje de WhatsApp, lo divide y envía en varios mensajes seguidos
    (Meta rechaza en silencio los mensajes demasiado largos).
    """
    exito = True
    for parte in dividir_mensaje_whatsapp(mensaje_texto):
        exito = _enviar_whatsapp_una_parte(numero_destino, parte) and exito
    return exito

# ============================================================================
# FASE 2: CUESTIONARIOS AUTOMÁTICOS DE PROYECCIÓN Y ALERTAS DE DESVIACIÓN
# ============================================================================

def destinatarios_envios(roles):
    """Destinatarios de un envío automático según rol. En modo prueba
    (ENVIOS_AUTOMATICOS_PRODUCCION != "1") todo envío va SOLO a los admin,
    sin importar los roles pedidos. Gerencia y EAS quedan fuera salvo que tengan
    activadas las notificaciones."""
    if not ENVIOS_AUTOMATICOS_PRODUCCION:
        return numeros_por_rol(("admin",))
    return numeros_por_rol(roles, solo_con_notificaciones=True)

def _sin_fundos_asignados(destinatario):
    """True si es un rol acotado (zonal/productor) sin fundos: NO debe recibir el envío.
    Sin esta comprobación una lista de fundos vacía no filtra nada y la persona recibiría
    lo previsto de todos los productores."""
    rol = (destinatario.get("rol") or ROL_POR_DEFECTO)
    return rol in ROLES_ACOTADOS and not destinatario.get("fundos")

def _fundo_coincide(fundo_asignado, texto):
    """¿El fundo asignado aparece en ese texto? (comparación laxa, como los filtros LIKE)."""
    if not fundo_asignado or not texto:
        return False
    return fundo_asignado.strip().upper() in str(texto).upper()

def registrar_cuestionario_enviado(numero, turno, mensaje):
    conn = sqlite3.connect(DB_LOCAL_PATH)
    # Un cuestionario nuevo deja obsoleto cualquier pendiente anterior del mismo número
    conn.execute(
        "UPDATE cuestionarios SET estado = 'vencido' WHERE numero = ? AND estado = 'pendiente'",
        (normalizar_numero(numero),)
    )
    conn.execute(
        "INSERT INTO cuestionarios (numero, turno, mensaje) VALUES (?, ?, ?)",
        (normalizar_numero(numero), turno, mensaje)
    )
    conn.commit()
    conn.close()

def obtener_cuestionario_activo(numero):
    """Último cuestionario de las últimas 24 h para ese número, esté pendiente O ya
    respondido: mientras siga vigente, el usuario puede corregir su respuesta (ej. ajustó
    en la mañana y a las horas quiere cambiar la cifra). None si no hay ninguno vigente."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        fila = conn.execute(
            "SELECT id, turno, mensaje, estado, respuesta, ajuste, fecha_hora_envio FROM cuestionarios "
            "WHERE numero = ? AND estado != 'vencido' "
            "AND fecha_hora_envio >= datetime('now', 'localtime', '-1 day') "
            "ORDER BY id DESC LIMIT 1",
            (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return dict(fila) if fila else None
    except Exception as e:
        logger.error(f"Error buscando cuestionario activo: {str(e)}")
        return None

def registrar_respuesta_cuestionario(numero, resultado, detalle_ajuste=None, texto_usuario=None):
    """Registra (o corrige) la respuesta del cuestionario vigente del número. Mientras el
    cuestionario esté dentro de sus 24 h se puede responder de nuevo: la corrección REEMPLAZA
    lo registrado y vuelve a avisar al EAS. La llama Claude vía la herramienta
    registrar_respuesta_cuestionario; lo que retorna se le envía al usuario."""
    if not numero:
        return "No pude asociar tu reporte a un número de origen."
    if resultado not in ("confirmado", "ajustado"):
        resultado = "ajustado"
    activo = obtener_cuestionario_activo(numero)
    if not activo and resultado == "confirmado":
        # "confirmo" sin cuestionario que confirmar no tiene qué registrar.
        return "No tienes ningún cuestionario de proyección vigente para confirmar."
    es_correccion = bool(activo) and activo.get("estado") != "pendiente"
    es_espontaneo = not activo
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        if activo:
            conn.execute(
                "UPDATE cuestionarios SET estado = ?, respuesta = ?, ajuste = ?, "
                "fecha_hora_respuesta = datetime('now', 'localtime') WHERE id = ?",
                (resultado, texto_usuario, detalle_ajuste, activo["id"])
            )
        else:
            # Reporte fuera de cuestionario: se guarda como una fila propia, con turno
            # 'espontaneo', para que quede en el mismo historial que el resto.
            conn.execute(
                "INSERT INTO cuestionarios (numero, turno, mensaje, estado, respuesta, ajuste, "
                "fecha_hora_respuesta) VALUES (?, 'espontaneo', ?, ?, ?, ?, datetime('now', 'localtime'))",
                (
                    normalizar_numero(numero),
                    "(reporte espontáneo del usuario, sin cuestionario previo)",
                    resultado,
                    texto_usuario,
                    detalle_ajuste,
                )
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando respuesta de cuestionario: {str(e)}")
        return "Tuve un problema registrando tu reporte. Intenta de nuevo en un momento."

    if resultado == "ajustado":
        notificar_ajuste_cuestionario(
            numero, detalle_ajuste or texto_usuario or "(sin detalle)", es_correccion=es_correccion
        )
        detalle = f":\n{detalle_ajuste}" if detalle_ajuste else "."
        if es_correccion:
            return f"✅ Corrección registrada{detalle}\nGracias, reemplacé lo anterior y le avisamos al equipo EAS."
        if es_espontaneo:
            return f"✅ Reporte registrado{detalle}\nGracias, quedó guardado y le avisamos al equipo EAS."
        return f"✅ Ajuste registrado{detalle}\nGracias, quedó guardado y le avisamos al equipo EAS."
    if es_correccion:
        return "✅ Listo, dejé la proyección registrada como confirmada (sin ajustes)."
    return "✅ Proyección confirmada, quedó registrada. ¡Gracias!"

def notificar_ajuste_cuestionario(numero_origen, detalle, es_correccion=False):
    """Aviso inmediato cuando alguien ajusta su proyección: siempre al admin, a gerencia y
    EAS si tienen las notificaciones activadas, y a los zonales que tengan asignado alguno
    de los fundos de quien reporta (un zonal solo se entera de lo suyo)."""
    try:
        num = normalizar_numero(numero_origen)
        info = next((n for n in listar_numeros_permitidos() if n["numero"] == num), None)
        quien = (info.get("nombre") if info else None) or num
        fundos_origen = fundos_de(num)
        if es_correccion:
            mensaje = f"📝 CORRECCIÓN de proyección de {quien} (+{num}), reemplaza su reporte anterior:\n\n{detalle}"
        else:
            mensaje = f"📝 Ajuste de proyección reportado por {quien} (+{num}):\n\n{detalle}"

        destinatarios = destinatarios_envios(("eas", "admin", "gerencia"))
        if ENVIOS_AUTOMATICOS_PRODUCCION:
            # Zonales: solo los que comparten fundo con quien reporta.
            for zonal in numeros_por_rol(("zonal",)):
                comparte = any(
                    _fundo_coincide(f_zonal, f_origen)
                    for f_zonal in zonal["fundos"] for f_origen in fundos_origen
                )
                if comparte and zonal["numero"] not in {d["numero"] for d in destinatarios}:
                    destinatarios.append(zonal)
        destinatarios = [d for d in destinatarios if d["numero"] != num]
        # Solo se registran los que Meta aceptó: antes se anotaba el aviso como enviado
        # aunque el envío hubiera fallado, y el panel mostraba entregas que nunca ocurrieron.
        aceptados = [d for d in destinatarios if enviar_whatsapp(d["numero"], mensaje)]
        if aceptados:
            registrar_alerta_enviada("ajuste_cuestionario", mensaje, aceptados)
        if len(aceptados) < len(destinatarios):
            fallidos = [d["numero"] for d in destinatarios if d not in aceptados]
            logger.error(f"Aviso de ajuste no aceptado para: {', '.join(fallidos)}")
    except Exception as e:
        logger.error(f"Error notificando ajuste de cuestionario: {str(e)}")

MAX_SECCIONES_CUESTIONARIO = 12

def obtener_previsto(fecha_inicio, fecha_fin, fundos=None):
    """Filas previstas por la fuente del cuestionario (Trisemanal) entre dos fechas, como
    (productor, especie, envase, kg, bultos). La base trae filas duplicadas del mismo
    registro —una con Bultos y KgsRecepcionados NULL y otra con ambos—, así que se filtran
    las de kg NULL: sin eso los bultos se duplican (verificado 15-01-2026: 2.203 vs 1.712)."""
    conn = conectar_sql()
    if not conn:
        return None
    try:
        condiciones = [
            "[Base Origen] = ?",
            "CAST(Fecha AS DATE) BETWEEN ? AND ?",
            "KgsRecepcionados IS NOT NULL",
        ]
        params = [
            BASE_ORIGEN_CUESTIONARIO,
            fecha_inicio.strftime("%Y-%m-%d"),
            fecha_fin.strftime("%Y-%m-%d"),
        ]
        # `fundos` acota a los fundos de esa persona (un número puede tener varios).
        fundos = [f for f in (fundos or []) if f]
        if fundos:
            condiciones.append("(" + " OR ".join(["Productor LIKE ?"] * len(fundos)) + ")")
            params.extend(f"%{f}%" for f in fundos)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT Productor, Especie, Envase,
                   SUM(KgsRecepcionados) as kg, SUM(Bultos) as bultos
            FROM [Recepcion_Consolidada]
            WHERE {" AND ".join(condiciones)}
            GROUP BY Productor, Especie, Envase
            HAVING SUM(KgsRecepcionados) > 0
            ORDER BY Productor, SUM(KgsRecepcionados) DESC
        """, params)
        return cursor.fetchall()
    finally:
        conn.close()

def _tabla_previsto(filas_productor):
    """Tabla Especie | Envase | Kilos | Bultos con su total, para un productor."""
    anchos = {"especie": 10, "envase": 8, "kg": 10, "bultos": 8}
    lineas = [
        f"{'Especie':<{anchos['especie']}}{'Envase':<{anchos['envase']}}"
        f"{'Kilos':>{anchos['kg']}}{'Bultos':>{anchos['bultos']}}"
    ]
    total_kg = total_bultos = 0
    for especie, envase, kg, bultos in filas_productor:
        total_kg += kg or 0
        total_bultos += bultos or 0
        lineas.append(
            # Envase viene de un campo de ancho fijo: sin strip() llega con espacios de
            # relleno y _truncar lo corta como si no cupiera ("BINS   …").
            f"{_truncar(traducir_especie(especie), anchos['especie']):<{anchos['especie']}}"
            f"{_truncar(str(envase or '-').strip(), anchos['envase']):<{anchos['envase']}}"
            f"{formatear_kg(kg):>{anchos['kg']}}{formatear_kg(bultos):>{anchos['bultos']}}"
        )
    lineas.append(
        f"{'TOTAL':<{anchos['especie'] + anchos['envase']}}"
        f"{formatear_kg(total_kg):>{anchos['kg']}}{formatear_kg(total_bultos):>{anchos['bultos']}}"
    )
    return "\n".join(lineas), total_kg, total_bultos

def _agrupar_por_productor(filas):
    agrupado = {}
    for productor, especie, envase, kg, bultos in filas:
        agrupado.setdefault(productor, []).append((especie, envase, kg, bultos))
    return agrupado

def construir_cuestionario_diario(fecha=None, nombre=None, fundos=None):
    """Mensaje de las 08:00: lo previsto para ese día por productor, con el formato
    acordado (Especie | Envase | Kilos | Bultos). None si no hay nada previsto ese día."""
    try:
        fecha = fecha or date.today()
        filas = obtener_previsto(fecha, fecha, fundos=fundos)
        if not filas:
            return None
        agrupado = _agrupar_por_productor(filas)
        saludo = f"Hola {nombre.strip()}" if nombre and nombre.strip() else "Hola"
        lineas = [
            f"{saludo}, para hoy tienes prevista una Precosecha "
            f"({ETIQUETA_FUENTE[BASE_ORIGEN_CUESTIONARIO].lower()}) por Productor de:",
            "",
        ]
        for prod in list(agrupado)[:MAX_SECCIONES_CUESTIONARIO]:
            tabla, _, _ = _tabla_previsto(agrupado[prod])
            lineas.append(f"Productor {prod}:")
            lineas.append(f"```{tabla}```")
            lineas.append("")
        if len(agrupado) > MAX_SECCIONES_CUESTIONARIO:
            lineas.append(
                f"(se muestran {MAX_SECCIONES_CUESTIONARIO} de {len(agrupado)} productores)"
            )
            lineas.append("")
        lineas.append("¿Se confirma cantidades previstas?")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error construyendo cuestionario diario: {str(e)}")
        return None

def construir_confirmacion_tarde(fecha=None, nombre=None, fundos=None, ajuste_previo=None):
    """Mensaje de las 15:00: repite lo que quedó informado en la mañana (lo previsto, más
    el ajuste que el usuario haya reportado) y pide confirmarlo al cierre del día."""
    try:
        fecha = fecha or date.today()
        filas = obtener_previsto(fecha, fecha, fundos=fundos)
        if not filas:
            return None
        agrupado = _agrupar_por_productor(filas)
        saludo = f"Hola {nombre.strip()}" if nombre and nombre.strip() else "Hola"
        lineas = [f"{saludo}, esta mañana quedó informado para hoy:", ""]
        for prod in list(agrupado)[:MAX_SECCIONES_CUESTIONARIO]:
            tabla, _, _ = _tabla_previsto(agrupado[prod])
            lineas.append(f"Productor {prod}:")
            lineas.append(f"```{tabla}```")
            lineas.append("")
        if ajuste_previo:
            lineas.append(f"Ajuste que reportaste hoy: {ajuste_previo}")
            lineas.append("")
        lineas.append("¿Confirmas estas cantidades al cierre del día?")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error construyendo confirmación de tarde: {str(e)}")
        return None

def _ajuste_registrado_hoy(numero, fecha=None):
    """Ajuste que el número reportó hoy (para repetirlo en el mensaje de las 15:00)."""
    try:
        fecha = fecha or date.today()
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT ajuste FROM cuestionarios WHERE numero = ? AND estado = 'ajustado' "
            "AND date(fecha_hora_respuesta) = ? AND ajuste IS NOT NULL "
            "ORDER BY id DESC LIMIT 1",
            (normalizar_numero(numero), fecha.strftime("%Y-%m-%d"))
        ).fetchone()
        conn.close()
        return fila[0] if fila else None
    except Exception as e:
        logger.error(f"Error buscando ajuste del día: {str(e)}")
        return None

def enviar_cuestionarios(turno, fecha=None):
    """Job programado: 'diario_am' (08:00) manda lo previsto del día y pide confirmar;
    'diario_pm' (15:00) repite lo informado en la mañana y pide confirmarlo al cierre.
    Va a zonales y productores (solo a los admin mientras dure el modo prueba)."""
    try:
        fecha = fecha or date.today()
        destinatarios = destinatarios_envios(("zonal", "productor"))
        if not destinatarios:
            logger.warning("Cuestionario: no hay destinatarios (¿ningún número con el rol requerido?)")
            return {"enviados": 0, "sin_datos": 0, "errores": 0, "detalle": "sin destinatarios"}
        enviados = sin_datos = errores = omitidos = 0
        for d in destinatarios:
            if _sin_fundos_asignados(d):
                omitidos += 1
                logger.warning(
                    f"Cuestionario {turno}: se omite {d['numero']} ({d.get('nombre')}), "
                    f"rol {d.get('rol')} sin fundos asignados"
                )
                continue
            if turno == "diario_pm":
                mensaje = construir_confirmacion_tarde(
                    fecha,
                    nombre=d.get("nombre"),
                    fundos=d.get("fundos"),
                    ajuste_previo=_ajuste_registrado_hoy(d["numero"], fecha),
                )
            else:
                mensaje = construir_cuestionario_diario(
                    fecha, nombre=d.get("nombre"), fundos=d.get("fundos")
                )
            if not mensaje:
                sin_datos += 1
                logger.info(f"Cuestionario {turno}: sin previsto para {d['numero']} (fundos={d.get('fundos')})")
                continue
            if enviar_whatsapp(d["numero"], mensaje):
                registrar_cuestionario_enviado(d["numero"], turno, mensaje)
                enviados += 1
            else:
                # Falla típica: ventana de 24 h cerrada (error 131047). Para producción hay
                # que aprobar una plantilla de re-enganche en Meta, como la de bienvenida.
                errores += 1
        logger.info(
            f"Cuestionario {turno}: {enviados} enviados, {sin_datos} sin datos, "
            f"{errores} errores, {omitidos} omitidos por no tener fundos"
        )
        return {"enviados": enviados, "sin_datos": sin_datos, "errores": errores,
                "omitidos_sin_fundos": omitidos}
    except Exception as e:
        logger.error(f"Error en job de cuestionarios: {str(e)}")
        return {"error": str(e)}

def _lunes_de(fecha):
    return fecha - timedelta(days=fecha.weekday())

def construir_resumen_semanal(fecha=None, nombre=None, fundos=None, para_eas=False):
    """Mensaje de los lunes 08:00: total previsto de la semana. Para el productor va su
    propio desglose por especie; para el EAS, el consolidado por productor."""
    try:
        fecha = fecha or date.today()
        inicio = _lunes_de(fecha)
        fin = inicio + timedelta(days=6)
        filas = obtener_previsto(inicio, fin, fundos=fundos)
        if not filas:
            return None
        rango = f"del {inicio.strftime('%d-%m')} al {fin.strftime('%d-%m')}"
        etiqueta = ETIQUETA_FUENTE[BASE_ORIGEN_CUESTIONARIO].lower()

        if para_eas:
            por_productor = {}
            for prod, _especie, _envase, kg, bultos in filas:
                acum = por_productor.setdefault(prod, [0, 0])
                acum[0] += kg or 0
                acum[1] += bultos or 0
            anchos = {"prod": 22, "kg": 11, "bultos": 8}
            tabla = [
                f"{'Productor':<{anchos['prod']}}{'Kilos':>{anchos['kg']}}{'Bultos':>{anchos['bultos']}}"
            ]
            tot_kg = tot_bultos = 0
            for prod, (kg, bultos) in sorted(por_productor.items(), key=lambda x: x[1][0], reverse=True):
                tot_kg += kg
                tot_bultos += bultos
                tabla.append(
                    f"{_truncar(str(prod), anchos['prod']):<{anchos['prod']}}"
                    f"{formatear_kg(kg):>{anchos['kg']}}{formatear_kg(bultos):>{anchos['bultos']}}"
                )
            tabla.append(
                f"{'TOTAL':<{anchos['prod']}}{formatear_kg(tot_kg):>{anchos['kg']}}"
                f"{formatear_kg(tot_bultos):>{anchos['bultos']}}"
            )
            return "\n".join([
                f"📅 Precosecha ({etiqueta}) prevista para la semana {rango}",
                "",
                f"```{chr(10).join(tabla)}```",
                "",
                f"{len(por_productor)} productores. Mensaje informativo, no requiere respuesta.",
            ])

        agrupado = _agrupar_por_productor(filas)
        saludo = f"Hola {nombre.strip()}" if nombre and nombre.strip() else "Hola"
        lineas = [
            f"{saludo}, esta es tu Precosecha ({etiqueta}) prevista para la semana {rango}:",
            "",
        ]
        for prod in list(agrupado)[:MAX_SECCIONES_CUESTIONARIO]:
            tabla, _, _ = _tabla_previsto(agrupado[prod])
            lineas.append(f"Productor {prod}:")
            lineas.append(f"```{tabla}```")
            lineas.append("")
        lineas.append("Mensaje informativo, no requiere respuesta.")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error construyendo resumen semanal: {str(e)}")
        return None

def enviar_resumen_semanal(fecha=None):
    """Job de los lunes 08:00: su semana a cada productor/zonal, y el consolidado al EAS
    (todo solo a los admin mientras dure el modo prueba)."""
    try:
        fecha = fecha or date.today()
        enviados = sin_datos = errores = omitidos = 0
        for d in destinatarios_envios(("zonal", "productor")):
            if _sin_fundos_asignados(d):
                omitidos += 1
                logger.warning(
                    f"Resumen semanal: se omite {d['numero']} ({d.get('nombre')}), "
                    f"rol {d.get('rol')} sin fundos asignados"
                )
                continue
            mensaje = construir_resumen_semanal(fecha, nombre=d.get("nombre"), fundos=d.get("fundos"))
            if not mensaje:
                sin_datos += 1
                continue
            if enviar_whatsapp(d["numero"], mensaje):
                enviados += 1
            else:
                errores += 1

        enviados_eas = 0
        mensaje_eas = construir_resumen_semanal(fecha, para_eas=True)
        destinatarios_eas = destinatarios_envios(("eas", "admin", "gerencia"))
        if mensaje_eas:
            for d in destinatarios_eas:
                if enviar_whatsapp(d["numero"], mensaje_eas):
                    enviados_eas += 1
            if destinatarios_eas:
                registrar_alerta_enviada("resumen_semanal_eas", mensaje_eas, destinatarios_eas)
        logger.info(
            f"Resumen semanal: {enviados} a productores, {enviados_eas} al EAS, "
            f"{sin_datos} sin datos, {omitidos} omitidos por no tener fundos"
        )
        return {"enviados_productores": enviados, "enviados_eas": enviados_eas,
                "sin_datos": sin_datos, "errores": errores, "omitidos_sin_fundos": omitidos}
    except Exception as e:
        logger.error(f"Error en job de resumen semanal: {str(e)}")
        return {"error": str(e)}

def construir_alerta_desviaciones():
    """Compara real acumulado vs estimado (Precosecha) acumulado a la fecha por variedad,
    para la temporada vigente. Retorna (texto, cantidad_desviadas) o None si no hay datos."""
    try:
        if not TEMPORADA_ACTUAL:
            return None
        conn = conectar_sql()
        if not conn:
            return None
        inicio, _ = rango_temporada(TEMPORADA_ACTUAL)
        ayer = date.today() - timedelta(days=1)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Variedad, [Base Origen], SUM(KgsRecepcionados) as total
            FROM [Recepcion_Consolidada]
            WHERE [Base Origen] IN (?, ?) AND CAST(Fecha AS DATE) BETWEEN ? AND ?
            GROUP BY Variedad, [Base Origen]
            HAVING SUM(KgsRecepcionados) > 0
        """, [BASE_ORIGEN_ESTIMADO, BASE_ORIGEN_REAL, inicio.strftime("%Y-%m-%d"), ayer.strftime("%Y-%m-%d")])
        filas = cursor.fetchall()

        variedades_activas = None
        if ALERTA_DIAS_ACTIVIDAD > 0:
            desde_actividad = (date.today() - timedelta(days=ALERTA_DIAS_ACTIVIDAD)).strftime("%Y-%m-%d")
            cursor.execute("""
                SELECT DISTINCT Variedad
                FROM [Recepcion_Consolidada]
                WHERE [Base Origen] IN (?, ?) AND CAST(Fecha AS DATE) >= ?
            """, [BASE_ORIGEN_ESTIMADO, BASE_ORIGEN_REAL, desde_actividad])
            variedades_activas = {f[0] for f in cursor.fetchall()}
        conn.close()
        if not filas:
            return None

        datos = {}
        for variedad, origen, total in filas:
            d = datos.setdefault(variedad, {"est": 0, "real": 0})
            if origen == BASE_ORIGEN_ESTIMADO:
                d["est"] = total or 0
            else:
                d["real"] = total or 0

        desviadas = []
        inactivas_desviadas = 0
        con_estimado = 0
        for variedad, d in datos.items():
            if d["est"] < ALERTA_MIN_KG:
                continue
            con_estimado += 1
            desv = (d["real"] - d["est"]) / d["est"] * 100
            if abs(desv) <= UMBRAL_DESVIACION_PCT:
                continue
            if variedades_activas is not None and variedad not in variedades_activas:
                inactivas_desviadas += 1
                continue
            desviadas.append((variedad, d["est"], d["real"], desv))
        # Orden por diferencia absoluta en kg (no por %): una desviación de -28% en una
        # variedad de 2,8 millones de kg importa más que un +145% en una de 55 mil.
        desviadas.sort(key=lambda x: abs(x[2] - x[1]), reverse=True)

        umbral = f"{UMBRAL_DESVIACION_PCT:g}"
        encabezado = (
            f"🚨 Alerta semanal de desviaciones (>{umbral}%)\n"
            f"Temporada {TEMPORADA_ACTUAL}, acumulado al {ayer.strftime('%d-%m-%Y')}\n"
            f"Real vs {ETIQUETA_FUENTE[BASE_ORIGEN_ESTIMADO]} a la fecha"
        )
        nota_inactivas = ""
        if inactivas_desviadas:
            nota_inactivas = (
                f"\nAdemás hay {inactivas_desviadas} variedad(es) desviadas SIN movimiento en los "
                f"últimos {ALERTA_DIAS_ACTIVIDAD} días (cosecha terminada o no iniciada), no listadas."
            )
        if not desviadas:
            texto = (
                f"{encabezado}\n\n✅ Sin variedades activas con desviación sobre {umbral}%."
                f"{nota_inactivas}"
            )
            return texto, 0

        anchos = {"variedad": 15, "num": 10, "pct": 7}
        filas_tabla = [
            f"{'Variedad':<{anchos['variedad']}}{'Estim':>{anchos['num']}}{'Real':>{anchos['num']}}{'Desv%':>{anchos['pct']}}"
        ]
        for variedad, est, real, desv in desviadas[:ALERTA_MAX_FILAS]:
            filas_tabla.append(
                f"{_truncar(str(variedad), anchos['variedad']):<{anchos['variedad']}}"
                f"{formatear_kg(est):>{anchos['num']}}{formatear_kg(real):>{anchos['num']}}"
                f"{desv:>+{anchos['pct'] - 1}.0f}%"
            )
        nota_tope = ""
        if len(desviadas) > ALERTA_MAX_FILAS:
            nota_tope = f" (se muestran las {ALERTA_MAX_FILAS} con mayor diferencia en kg)"
        texto = (
            f"{encabezado}\n"
            f"```{chr(10).join(filas_tabla)}```\n"
            f"{len(desviadas)} variedad(es) activas fuera de rango, de {con_estimado} con estimado a la fecha{nota_tope}."
            f"{nota_inactivas}"
        )
        return texto, len(desviadas)
    except Exception as e:
        logger.error(f"Error construyendo alerta de desviaciones: {str(e)}")
        return None

def registrar_alerta_enviada(tipo, contenido, destinatarios):
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute(
            "INSERT INTO alertas_enviadas (tipo, contenido, destinatarios) VALUES (?, ?, ?)",
            (tipo, contenido, ", ".join(d["numero"] for d in destinatarios))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando alerta enviada: {str(e)}")

def enviar_alerta_desviaciones():
    """Job programado (semanal): envía al EAS (solo admin en modo prueba) el resumen de
    variedades desviadas más de UMBRAL_DESVIACION_PCT % respecto del estimado."""
    try:
        resultado = construir_alerta_desviaciones()
        if not resultado:
            logger.warning("Alerta de desviaciones: sin datos para calcular")
            return {"enviados": 0, "detalle": "sin datos"}
        mensaje, num_desviadas = resultado
        destinatarios = destinatarios_envios(("eas", "admin", "gerencia"))
        if not destinatarios:
            logger.warning("Alerta de desviaciones: no hay destinatarios")
            return {"enviados": 0, "detalle": "sin destinatarios"}
        aceptados = [d for d in destinatarios if enviar_whatsapp(d["numero"], mensaje)]
        enviados = len(aceptados)
        if aceptados:
            registrar_alerta_enviada("desviacion_semanal", mensaje, aceptados)
        logger.info(f"Alerta de desviaciones: {enviados} enviados, {num_desviadas} variedades fuera de rango")
        return {"enviados": enviados, "variedades_desviadas": num_desviadas}
    except Exception as e:
        logger.error(f"Error en job de alerta de desviaciones: {str(e)}")
        return {"error": str(e)}

def _hora_cron(hhmm, por_defecto):
    try:
        h, m = hhmm.strip().split(":")
        return int(h), int(m)
    except Exception:
        return por_defecto

scheduler = BackgroundScheduler()

@app.on_event("startup")
def iniciar_scheduler():
    if scheduler.running:
        return
    h_am, m_am = _hora_cron(CUESTIONARIO_HORA_AM, (8, 0))
    h_pm, m_pm = _hora_cron(CUESTIONARIO_HORA_PM, (15, 0))
    h_sem, m_sem = _hora_cron(RESUMEN_SEMANAL_HORA, (8, 0))
    h_al, m_al = _hora_cron(ALERTA_SEMANAL_HORA, (8, 0))
    scheduler.add_job(enviar_cuestionarios, "cron", args=["diario_am"], hour=h_am, minute=m_am, id="cuestionario_am")
    scheduler.add_job(enviar_cuestionarios, "cron", args=["diario_pm"], hour=h_pm, minute=m_pm, id="cuestionario_pm")
    scheduler.add_job(enviar_resumen_semanal, "cron", day_of_week=RESUMEN_SEMANAL_DIA, hour=h_sem, minute=m_sem, id="resumen_semanal")
    scheduler.add_job(enviar_alerta_desviaciones, "cron", day_of_week=ALERTA_SEMANAL_DIA, hour=h_al, minute=m_al, id="alerta_semanal")
    scheduler.start()
    modo = "PRODUCCIÓN" if ENVIOS_AUTOMATICOS_PRODUCCION else "PRUEBA (solo números admin)"
    logger.info(
        f"Scheduler iniciado en modo {modo}: cuestionario diario {h_am:02d}:{m_am:02d} y "
        f"confirmación {h_pm:02d}:{m_pm:02d}, resumen semanal {RESUMEN_SEMANAL_DIA} {h_sem:02d}:{m_sem:02d}, "
        f"alerta desviaciones {ALERTA_SEMANAL_DIA} {h_al:02d}:{m_al:02d}"
    )

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

                    # Acuses de entrega de los mensajes que enviamos (sent/delivered/read/failed).
                    for status in value.get("statuses", []):
                        registrar_estado_entrega(status)

                    messages = value.get("messages", [])

                    for message in messages:
                        numero_sender = message.get("from")
                        msg_id = message.get("id")
                        msg_type = message.get("type")

                        if not numero_esta_permitido(numero_sender):
                            logger.warning(f"Mensaje rechazado (número no autorizado): {numero_sender}")
                            enviar_whatsapp(
                                numero_sender,
                                "No tienes acceso a este asistente. Si deberías tenerlo, contacta a quien "
                                "lo administra (Edson Lazo +56954482135) para solicitar acceso."
                            )
                            continue

                        if msg_type == "text":
                            msg_text = message.get("text", {}).get("body", "")
                        elif msg_type == "audio":
                            try:
                                media_id = message.get("audio", {}).get("id")
                                audio_bytes, mime_type = descargar_audio_whatsapp(media_id)
                                msg_text = transcribir_audio(audio_bytes, mime_type)
                                logger.info(f"Nota de voz transcrita de {numero_sender}: {msg_text}")
                            except Exception as e:
                                logger.error(f"Error transcribiendo audio: {str(e)}")
                                enviar_whatsapp(
                                    numero_sender,
                                    "No pude entender tu nota de voz. ¿Puedes escribir tu pregunta como texto?"
                                )
                                continue
                        else:
                            enviar_whatsapp(
                                numero_sender,
                                "Por ahora solo puedo responder mensajes de texto o notas de voz."
                            )
                            continue

                        logger.info(f"Mensaje de {numero_sender}: {msg_text}")

                        # Procesar mensaje
                        respuesta = procesar_mensaje(msg_text, numero_sender, es_audio=(msg_type == "audio"))

                        # Guardar en el historial local
                        guardar_conversacion(numero_sender, msg_type, msg_text, respuesta)

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

@app.get("/historial")
async def historial(clave: str, limit: int = 50):
    """
    Ver las últimas conversaciones registradas (protegido con clave).
    Uso: https://bot-whatsapp-asa.com/historial?clave=Matias14&limit=100
    """
    if clave != HISTORIAL_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT numero_sender, tipo_mensaje, mensaje, respuesta, fecha_hora "
            "FROM conversaciones ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        filas = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return {"total": len(filas), "conversaciones": filas}
    except Exception as e:
        logger.error(f"Error obteniendo historial: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/numeros")
async def admin_listar_numeros(clave: str):
    """
    Lista los números con acceso al bot (protegido con clave).
    Uso: https://bot-whatsapp-asa.com/admin/numeros?clave=...
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        numeros = listar_numeros_permitidos()
        return {"total": len(numeros), "numeros": numeros}
    except Exception as e:
        logger.error(f"Error listando números permitidos: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

PLANTILLA_BIENVENIDA_NOMBRE = "bienvenida_asistente_cosecha"
PLANTILLA_BIENVENIDA_IDIOMA = "en"  # idioma real aprobado en Meta (aunque el contenido esté en español)

def enviar_plantilla_bienvenida(numero_destino, nombre=None):
    """
    Envía la plantilla de WhatsApp "bienvenida_asistente_cosecha", aprobada por Meta. A
    diferencia de un mensaje de texto libre, una plantilla SÍ puede enviarse a un número que
    nunca le ha escrito al bot (no aplica la restricción de la ventana de 24 horas de
    WhatsApp — ver commit anterior: error 131047 "Re-engagement message").
    El contenido del mensaje vive en Meta (WhatsApp Manager > Plantillas de mensajes,
    id 1059735476635950) — cambiarlo aquí no tiene ningún efecto; hay que editarlo y
    volver a aprobarlo ahí.
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
            "type": "template",
            "template": {
                "name": PLANTILLA_BIENVENIDA_NOMBRE,
                "language": {"code": PLANTILLA_BIENVENIDA_IDIOMA},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {
                                "type": "text",
                                "parameter_name": "nombre",
                                "text": (nombre or "").strip() or "equipo",
                            }
                        ],
                    }
                ],
            },
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            logger.error(f"Plantilla de bienvenida respondió {response.status_code} al enviar a {numero_destino}: {response.text}")
        else:
            logger.info(f"Plantilla de bienvenida enviada a {numero_destino}: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error enviando plantilla de bienvenida: {str(e)}")
        return False

def _revisar_parametros(request, permitidos):
    """Devuelve un error si la URL trae parámetros que el endpoint no conoce. Sin esto, un
    nombre mal escrito (ej. 'notificacion' por 'notificaciones') se ignora en silencio y la
    llamada responde 'ok' sin haber hecho el cambio pedido."""
    desconocidos = [p for p in request.query_params if p not in permitidos]
    if not desconocidos:
        return None
    return JSONResponse(
        {
            "status": "error",
            "error": f"No conozco este parámetro: {', '.join(desconocidos)}. "
                     f"No se hizo ningún cambio.",
            "parametros_validos": sorted(permitidos),
        },
        status_code=400,
    )

# Alias para los nombres que es fácil escribir distinto al teclear la URL a mano.
ALIAS_PARAMETROS = {
    "notificacion": "notificaciones",
    "notificacines": "notificaciones",
    "notif": "notificaciones",
    "fundo": "fundos",
}

def _con_alias(request, valores):
    """Aplica los alias de parámetros sobre los valores ya recibidos."""
    for alias, real in ALIAS_PARAMETROS.items():
        if alias in request.query_params and not valores.get(real):
            valores[real] = request.query_params[alias]
    return valores

def _parsear_si_no(valor):
    """Acepta las formas que uno escribe a mano en la URL (1/0, si/no, true/false, on/off).
    Devuelve 1, 0, o None si no vino el parámetro. Lanza ValueError si no se entiende."""
    if valor is None or str(valor).strip() == "":
        return None
    texto = str(valor).strip().lower()
    if texto in ("1", "si", "sí", "true", "on", "activar", "activo", "yes"):
        return 1
    if texto in ("0", "no", "false", "off", "desactivar", "inactivo"):
        return 0
    raise ValueError(valor)

ERROR_SI_NO = "Valor inválido para 'notificaciones'. Usa 1/si/true para activar, o 0/no/false para desactivar."

def _estado_numero(numero):
    """Cómo quedó el número después de un cambio, para que la respuesta lo confirme en vez
    de solo decir 'ok' (antes no se veía si las notificaciones habían quedado activas)."""
    num = normalizar_numero(numero)
    rol = rol_de(num)
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT recibe_notificaciones FROM numeros_permitidos WHERE numero = ?", (num,)
        ).fetchone()
        conn.close()
        notif = bool(fila[0]) if fila else False
    except Exception:
        notif = False
    if rol == "admin":
        detalle_notif = "sí (los admin reciben siempre)"
    elif rol in ("gerencia", "eas"):
        detalle_notif = "sí" if notif else "no (actívalas con notificaciones=1)"
    else:
        detalle_notif = "sí, según su rol"
    return {
        "rol": rol,
        "fundos": fundos_de(num),
        "ve_todo": rol in ROLES_VEN_TODO,
        "recibe_notificaciones": notif,
        "notificaciones": detalle_notif,
    }

@app.get("/admin/numeros/agregar")
async def admin_agregar_numero(request: Request, clave: str, numero: str, nombre: str = None,
                               rol: str = None, productor: str = None, fundos: str = None,
                               notificaciones: str = None):
    """
    Da acceso a un número (protegido con clave). Si el número ya existía, actualiza solo los
    campos que vengan en la URL. Si es nuevo, además le envía la bienvenida por WhatsApp.
    rol: admin | gerencia | eas | zonal | productor (por defecto: productor).
    fundos: fundos asignados separados por "|" (para zonal/productor, que solo ven los suyos).
    notificaciones: 1/si para que gerencia o eas reciba los avisos automáticos.
    Uso: .../admin/numeros/agregar?clave=...&numero=56912345678&nombre=Matias&rol=zonal&fundos=LA TORINA
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    valores = _con_alias(request, {"fundos": fundos, "notificaciones": notificaciones})
    fundos, notificaciones = valores["fundos"], valores["notificaciones"]
    error = _revisar_parametros(
        request,
        {"clave", "numero", "nombre", "rol", "productor", "fundos", "notificaciones"} | set(ALIAS_PARAMETROS),
    )
    if error:
        return error
    if rol is not None and rol not in ROLES_VALIDOS:
        return JSONResponse(
            {"status": "error", "error": f"Rol inválido: {rol}. Válidos: {', '.join(ROLES_VALIDOS)}"},
            status_code=400,
        )
    try:
        notif = _parsear_si_no(notificaciones)
    except ValueError:
        return JSONResponse({"status": "error", "error": ERROR_SI_NO}, status_code=400)
    try:
        es_nuevo = not numero_esta_permitido(numero)
        agregar_numero_permitido(numero, nombre, rol=rol, productor=productor, recibe_notificaciones=notif)
        numero_normalizado = normalizar_numero(numero)
        if fundos is not None:
            asignar_fundos(numero_normalizado, fundos.split("|"), reemplazar=True)

        bienvenida_enviada = False
        if es_nuevo:
            bienvenida_enviada = enviar_plantilla_bienvenida(numero_normalizado, nombre)

        return {
            "status": "ok",
            "numero": numero_normalizado,
            "nombre": nombre,
            "nuevo": es_nuevo,
            "bienvenida_enviada": bienvenida_enviada,
            **_estado_numero(numero_normalizado),
        }
    except Exception as e:
        logger.error(f"Error agregando número permitido: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/numeros/rol")
async def admin_cambiar_rol(request: Request, clave: str, numero: str, rol: str = None,
                            productor: str = None, fundos: str = None, notificaciones: str = None):
    """
    Cambia el rol, los fundos asignados y/o si recibe notificaciones.
    Roles: admin (todo + avisos siempre), gerencia y eas (consultan todo; avisos solo si
    notificaciones=1), zonal y productor (solo SUS fundos).
    Uso: .../admin/numeros/rol?clave=...&numero=56912345678&rol=zonal&fundos=CARMELO|HUIQUE
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    valores = _con_alias(request, {"fundos": fundos, "notificaciones": notificaciones})
    fundos, notificaciones = valores["fundos"], valores["notificaciones"]
    error = _revisar_parametros(
        request,
        {"clave", "numero", "rol", "productor", "fundos", "notificaciones"} | set(ALIAS_PARAMETROS),
    )
    if error:
        return error
    if rol is None and productor is None and fundos is None and notificaciones is None:
        return JSONResponse(
            {"status": "error", "error": "Indica al menos rol, fundos, productor o notificaciones"},
            status_code=400,
        )
    if rol is not None and rol not in ROLES_VALIDOS:
        return JSONResponse(
            {"status": "error", "error": f"Rol inválido: {rol}. Válidos: {', '.join(ROLES_VALIDOS)}"},
            status_code=400,
        )
    try:
        notif = _parsear_si_no(notificaciones)
    except ValueError:
        return JSONResponse({"status": "error", "error": ERROR_SI_NO}, status_code=400)
    try:
        if not numero_esta_permitido(numero):
            return JSONResponse({"status": "error", "error": "Ese número no está en la lista"}, status_code=404)
        agregar_numero_permitido(numero, rol=rol, productor=productor, recibe_notificaciones=notif)
        if fundos is not None:
            # Lista separada por "|" porque los nombres de fundo llevan comas y puntos.
            asignar_fundos(numero, [f for f in fundos.split("|")], reemplazar=True)
        num = normalizar_numero(numero)
        return {"status": "ok", "numero": num, **_estado_numero(num)}
    except Exception as e:
        logger.error(f"Error cambiando rol de número: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/numeros/fundos")
async def admin_fundos(clave: str, numero: str, agregar: str = None, quitar: str = None):
    """
    Agrega o quita fundos de un número sin tocar el resto (varios separados por "|").
    Solo afecta a zonal y productor: admin, gerencia y EAS ven todo igual.
    Uso: .../admin/numeros/fundos?clave=...&numero=56912345678&agregar=SANTA ANA DE HUIQUE
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    if not agregar and not quitar:
        return JSONResponse({"status": "error", "error": "Indica agregar y/o quitar"}, status_code=400)
    try:
        if not numero_esta_permitido(numero):
            return JSONResponse({"status": "error", "error": "Ese número no está en la lista"}, status_code=404)
        if agregar:
            asignar_fundos(numero, agregar.split("|"))
        if quitar:
            for f in quitar.split("|"):
                quitar_fundo(numero, f)
        num = normalizar_numero(numero)
        return {"status": "ok", "numero": num, "rol": rol_de(num), "fundos": fundos_de(num)}
    except Exception as e:
        logger.error(f"Error cambiando fundos: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/fundos/disponibles")
async def admin_fundos_disponibles(clave: str, buscar: str = None):
    """
    Lista los nombres de productor/fundo tal como están en la base, para asignarlos sin
    errores de tipeo. Con buscar=texto filtra la lista.
    Uso: .../admin/fundos/disponibles?clave=...&buscar=carmelo
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    fundos = PRODUCTORES_CONOCIDOS
    if buscar:
        fundos = [f for f in fundos if buscar.strip().upper() in f.upper()]
    return {"total": len(fundos), "fundos": fundos}

def _modo_envios():
    return "produccion" if ENVIOS_AUTOMATICOS_PRODUCCION else "prueba (solo números admin)"

def _parsear_fecha_param(fecha):
    """Convierte el parámetro ?fecha=YYYY-MM-DD, o None para usar hoy. Lanza ValueError."""
    if not fecha:
        return None
    return datetime.strptime(fecha, "%Y-%m-%d").date()

@app.get("/admin/cuestionario/enviar")
async def admin_enviar_cuestionario(clave: str, turno: str = "diario_am", solo_ver: bool = False, fecha: str = None):
    """
    Dispara manualmente el cuestionario diario (mismo envío que el job programado, respeta el
    modo prueba/producción). turno: 'diario_am' (lo previsto del día) o 'diario_pm' (confirmar
    lo informado). Con solo_ver=true muestra el texto sin enviar nada. fecha=YYYY-MM-DD permite
    probar con un día que sí tenga datos trisemanales (entre temporadas no hay previsto para hoy).
    Uso: .../admin/cuestionario/enviar?clave=...&turno=diario_am&solo_ver=true&fecha=2026-01-15
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    if turno not in ("diario_am", "diario_pm"):
        return JSONResponse({"status": "error", "error": "turno debe ser 'diario_am' o 'diario_pm'"}, status_code=400)
    try:
        dia = _parsear_fecha_param(fecha)
    except ValueError:
        return JSONResponse({"status": "error", "error": "fecha debe ser YYYY-MM-DD"}, status_code=400)
    if solo_ver:
        if turno == "diario_pm":
            texto = construir_confirmacion_tarde(dia, nombre="Edson")
        else:
            texto = construir_cuestionario_diario(dia, nombre="Edson")
        return {"status": "ok", "texto": texto or "(sin previsto ese día: no se enviaría nada)"}
    return {"status": "ok", "modo": _modo_envios(), "resultado": enviar_cuestionarios(turno, fecha=dia)}

@app.get("/admin/resumen/semanal")
async def admin_resumen_semanal(clave: str, solo_ver: bool = False, fecha: str = None):
    """
    Dispara manualmente el resumen semanal de precosecha de los lunes (a productores su
    semana, y el consolidado al EAS). Con solo_ver=true muestra ambos textos sin enviar.
    fecha=YYYY-MM-DD elige la semana (se usa el lunes de esa semana).
    Uso: .../admin/resumen/semanal?clave=...&solo_ver=true&fecha=2026-01-15
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        dia = _parsear_fecha_param(fecha)
    except ValueError:
        return JSONResponse({"status": "error", "error": "fecha debe ser YYYY-MM-DD"}, status_code=400)
    if solo_ver:
        return {
            "status": "ok",
            "texto_productor": construir_resumen_semanal(dia, nombre="Edson") or "(sin previsto esa semana)",
            "texto_eas": construir_resumen_semanal(dia, para_eas=True) or "(sin previsto esa semana)",
        }
    return {"status": "ok", "modo": _modo_envios(), "resultado": enviar_resumen_semanal(fecha=dia)}

@app.get("/admin/alerta/desviaciones")
async def admin_alerta_desviaciones(clave: str, solo_ver: bool = False):
    """
    Dispara manualmente la alerta semanal de desviaciones (respeta el modo prueba/producción).
    Con solo_ver=true muestra el texto sin enviar nada.
    Uso: https://bot-whatsapp-asa.com/admin/alerta/desviaciones?clave=...&solo_ver=true
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    if solo_ver:
        resultado = construir_alerta_desviaciones()
        return {"status": "ok", "texto": resultado[0] if resultado else "(sin datos para calcular)"}
    return {"status": "ok", "modo": _modo_envios(), "resultado": enviar_alerta_desviaciones()}

@app.get("/admin/entregas")
async def admin_entregas(clave: str, limit: int = 40, solo_fallidos: bool = False):
    """
    Estado REAL de entrega de los últimos mensajes enviados por el bot. Meta responde 200 al
    aceptar un mensaje, pero si no se entrega eso solo llega por los statuses del webhook:
    aquí se ven como 'failed' con el código de error de Meta.
    Uso: https://bot-whatsapp-asa.com/admin/entregas?clave=...&solo_fallidos=true
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        filtro = "WHERE m.estado = 'failed'" if solo_fallidos else ""
        cursor = conn.execute(
            f"SELECT m.wamid, m.numero, n.nombre, m.estado, m.error_code, m.error_detalle, "
            f"m.fecha_envio, m.fecha_estado "
            f"FROM mensajes_estado m LEFT JOIN numeros_permitidos n ON n.numero = m.numero "
            f"{filtro} ORDER BY m.rowid DESC LIMIT ?",
            (limit,)
        )
        filas = [dict(row) for row in cursor.fetchall()]
        resumen = {}
        for row in conn.execute("SELECT estado, COUNT(*) c FROM mensajes_estado GROUP BY estado"):
            resumen[row["estado"]] = row["c"]
        conn.close()
        return {"resumen_por_estado": resumen, "total_listado": len(filas), "mensajes": filas}
    except Exception as e:
        logger.error(f"Error listando entregas: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/cuestionario/respuestas")
async def admin_respuestas_cuestionario(clave: str, limit: int = 30):
    """
    Últimos cuestionarios enviados con su estado y respuesta (para revisar qué contestó cada uno).
    Uso: https://bot-whatsapp-asa.com/admin/cuestionario/respuestas?clave=...
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT c.id, c.numero, n.nombre, c.turno, c.estado, c.respuesta, c.ajuste, "
            "c.fecha_hora_envio, c.fecha_hora_respuesta "
            "FROM cuestionarios c LEFT JOIN numeros_permitidos n ON n.numero = c.numero "
            "ORDER BY c.id DESC LIMIT ?",
            (limit,)
        )
        filas = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return {"total": len(filas), "cuestionarios": filas}
    except Exception as e:
        logger.error(f"Error listando respuestas de cuestionarios: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/numeros/quitar")
async def admin_quitar_numero(clave: str, numero: str):
    """
    Quita el acceso a un número (protegido con clave).
    Uso: https://bot-whatsapp-asa.com/admin/numeros/quitar?clave=...&numero=56912345678
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        eliminado = quitar_numero_permitido(numero)
        if not eliminado:
            return JSONResponse({"status": "error", "error": "Ese número no estaba en la lista"}, status_code=404)
        return {"status": "ok", "numero": normalizar_numero(numero)}
    except Exception as e:
        logger.error(f"Error quitando número permitido: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/test/mensaje")
async def test_mensaje(mensaje: str, numero: str = None, guardar: bool = False):
    """
    TEST LOCAL: Envía un mensaje y obtiene respuesta
    Usa: curl "http://localhost:8005/test/mensaje?mensaje=cuantos%20bins%20de%20tiffany"
    Para probar contexto entre mensajes: pasa el mismo &numero=... y &guardar=true en cada llamada.
    """
    respuesta = procesar_mensaje(mensaje, numero)
    if guardar and numero:
        guardar_conversacion(numero, "text", mensaje, respuesta)
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
