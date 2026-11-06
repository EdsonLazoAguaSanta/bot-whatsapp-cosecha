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
import re
import json
import sqlite3
import tempfile
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from pathlib import Path
from decimal import Decimal
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
# Modelo que conversa con los usuarios. Opus 5.5 es el más capaz para entender preguntas
# abiertas, encadenar varias consultas y analizar los resultados como un chat de IA. Se
# puede cambiar en el .env sin tocar el código.
CLAUDE_MODELO = os.getenv("CLAUDE_MODELO", "claude-opus-5-5")
# Profundidad de razonamiento: low | medium | high | xhigh | max. Opus 5.5 trae "medium" por
# defecto; "high" rinde mejor cuando una respuesta necesita varias consultas y análisis.
CLAUDE_ESFUERZO = os.getenv("CLAUDE_ESFUERZO", "high")
# Rondas de herramientas que puede encadenar Claude para responder UN mensaje (consultar,
# mirar el resultado, volver a consultar, armar el archivo...). Al llegar al tope se le pide
# que conteste con lo que ya tiene.
MAX_PASOS_HERRAMIENTAS = int(os.getenv("MAX_PASOS_HERRAMIENTAS", "10"))
# Si el modelo declina una solicitud por sus filtros de seguridad, la API la reintenta sola
# en otro modelo (fallback del lado del servidor). Solo lo aceptan estos modelos.
MODELOS_CON_FALLBACK = ("claude-opus-5-5", "claude-opus-5", "claude-fable-5-1", "claude-sonnet-5-5")
BETA_FALLBACK = "server-side-fallback-2026-07-01"

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
# Cuántas semanas hacia adelante cubre el resumen de los lunes.
RESUMEN_SEMANAS = int(os.getenv("RESUMEN_SEMANAS", "3"))
ALERTA_SEMANAL_DIA = os.getenv("ALERTA_SEMANAL_DIA", "mon")  # día en formato cron: mon, tue, ...
ALERTA_SEMANAL_HORA = os.getenv("ALERTA_SEMANAL_HORA", "08:00")
# Resumen de cierre a las plantas: lo previsto hoy para su planta con lo que confirmaron o
# ajustaron los zonales en la tarde (después del cuestionario de las 15:00).
RESUMEN_PLANTAS_HORA = os.getenv("RESUMEN_PLANTAS_HORA", "18:00")
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

# Asignación de productores a zonales entregada por Agua Santa (26-10-2026): (productor, CSG,
# grupo, zonal). Es solo la carga inicial de la tabla productores_zonales: después se
# mantiene con /admin/zonales/*. Al arrancar, cada nombre se reemplaza por el EXACTO que
# tiene en la base (ej. la planilla trae "VICTORIA CIA." y la base "VICTORIA  CIA.").
# Los que vienen sin CSG o todavía no tienen recepciones quedan igual: empiezan a calzar
# solos cuando aparezcan en la base con ese nombre.
ASIGNACION_ZONALES_INICIAL = [
    ("AGRICOLA ACRUX LIMITADA", "94323", "VALDES", "Joaquin Llano"),
    ("AGRICOLA AGUA DEL VALLE SPA (SG)", "174336", "AGUA DEL VALLE", "Pedro Dominguez"),
    ("AGRICOLA AGUA DEL VALLE SPA (SL-SE)", "102451", "AGUA DEL VALLE", "Pedro Dominguez"),
    ("AGRICOLA AGUA SANTA LTDA.", "89495", "AGUA SANTA (P Riesco)", "Pedro Dominguez"),
    ("AGRICOLA ANTARES LTDA. - PARCELA 29", "112218", "VALDES", "Joaquin Llano"),
    ("AGRICOLA ANTARES LTDA.- PARCELA 30", "112217", "VALDES", "Joaquin Llano"),
    ("AGRICOLA CERVERA LTDA.", "112514", "VALDES", "Joaquin Llano"),
    ("AGRÍCOLA E INMOBILIARIA EL CARMEN ( FUNDO SANTA NORMA)", "172788", "AGUA SANTA (P Riesco)", "Pedro Dominguez"),
    ("AGRÍCOLA E INVERSIONES DADUNEX LTDA.", "112538", "VALDES", "Joaquin Llano"),
    ("AGRÍCOLA E INVERSIONES DADUNEX LTDA. (PARCELA 44)", "172139", "VALDES", "Joaquin Llano"),
    ("AGRICOLA EL MILAGRO LTDA.", "153782", "G ACHURRA", "José Luis Castro"),
    ("AGRICOLA EL MILAGRO PARCELA 11", "88244", "G ACHURRA", "José Luis Castro"),
    ("AGRÍCOLA MALIHUITO SPA", "175854", "AGUA SANTA (P Riesco)", "Pedro Dominguez"),
    ("AGRICOLA SANTA FILOMENA LTDA.", "93445", "RODRIGUEZ", "Joaquin Llano"),
    ("AGRICOLA SANTA FILOMENA LTDA. - CUARTEL 19", "152068", "RODRIGUEZ", "Joaquin Llano"),
    ("AGRICOLA TRICAHUE LTDA. - TRICAHUE", "89738", "TRICAHUE", "Joaquin Llano"),
    ("AGRICOLA VILLASECA LIMITADA", "3101883", "AGUA SANTA (P Riesco)", "Pedro Dominguez"),
    ("AGRÍCOLA EL CARMELO LTDA", "117337", "AGUA SANTA (P Riesco)", "Pedro Dominguez"),
    ("EL CONDOR DE APALTA", "164368", "AGUA SANTA (P Rabat)", "Anjel Pino"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - CHANCON", "89918", "SUPERFRUIT", "Joaquin Llano"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - HUILMAY", "90080", "SUPERFRUIT", "Joaquin Llano"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - SAN PEDRO", "100044", "SUPERFRUIT", "Joaquin Llano"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - SANTA ELENA", "151141", "SUPERFRUIT", "Joaquin Llano"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA.", "89935", "SUPERFRUIT", "Joaquin Llano"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - SANTA RAQUEL", "100179", "SUPERFRUIT", "Joaquin Llano"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - SANTA ROSA", "100178", "SUPERFRUIT", "Joaquin Llano"),
    ("INVERSIONES LOS LOROS LTDA. - BUIN", "94367", "VALDES", "Joaquin Llano"),
    ("INVERSIONES LOS LOROS LTDA. - LLAY LLAY", "94467", "VALDES", "Joaquin Llano"),
    ("JOSE GUILFREDO GONZALEZ LEON", "112581", "VALDES", "Joaquin Llano"),
    ("LA TORINA", "155440", "AGUA SANTA (Garcia P)", "José Luis Castro"),
    ("LOS LOROS LIMITADA", "150871", "VALDES", "Joaquin Llano"),
    ("MARIA EUGENIA VALDES COVARRUBIAS", "94353", "VALDES", "Joaquin Llano"),
    ("SANTA AMELIA DE ALMAHUE", "89155", "AGUA SANTA (P Rabat)", "Anjel Pino"),
    ("SANTA ANA DE HUIQUE", "89494", "AGUA SANTA (P Rabat)", "Anjel Pino"),
    ("SOC. AGRÍCOLA LA RIOJA LTDA", "95167", "RISHMAWI", "José Luis Castro"),
    ("SOCIEDAD AGRICOLA SAN JORGE LTDA.", "121143", "VALDES", "Joaquin Llano"),
    ("VALLE MERCEDES DEL MAIPO S.A.", "150043", "RODRIGUEZ", "Joaquin Llano"),
    ("BELEN BUSTAMANTE", "3104516", "RIVERA", "José Luis Castro"),
    ("SOC. AGRICOLA EL CARMEN Y SAN JUAN-LAS PITRAS", "122940", "BALBONTIN", "José Luis Castro"),
    ("LA HIGUERA SA", "151595", "COVARRUBIAS", "Pedro Dominguez"),
    ("LA LISONJERA", "89681", "AGUA SANTA (P Riesco)", "Pedro Dominguez"),
    ("AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA. - LOS SUSPIROS", "172233", "SUPERFRUIT", "José Luis Castro"),
    ("AGRICOLA CAMPO BONITO SPA", "171923", "TOCORNAL", "Pedro Dominguez"),
    ("AGRICOLA LA PALMA S.A.", "115527", "LA PALMA", "José Luis Castro"),
    ("COMERCIALIZADORA VIVERO CORCOLEN LTDA", "3165969", "CORCOLEN", "José Luis Castro"),
    ("SOC.AGR. LECH.SANTA CRUZ DE LA VICTORIA CIA. LTDA.", None, "VALDES", "Joaquin Llano"),
    ("AGRICOLA ANTARES LTDA", None, "VALDES", "Joaquin Llano"),
    ("INVERSIONES LOS LOROS LTDA.", None, "VALDES", "Joaquin Llano"),
]

def inicializar_db_local():
    conn = sqlite3.connect(DB_LOCAL_PATH)
    # WAL: los mensajes se atienden en paralelo (un hilo por conversación) y así las
    # lecturas no bloquean a las escrituras ni al revés. Queda guardado en el archivo.
    conn.execute("PRAGMA journal_mode=WAL")
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
    # Cola de mensajes que no se pudieron mandar porque la ventana de 24 h estaba cerrada.
    # Se le envía una plantilla a la persona y su texto queda aquí hasta que responda; su
    # respuesta reabre la ventana y recién ahí se le entrega. Sirve para cualquier envío
    # automático (cuestionario, resumen semanal, alerta), no solo el cuestionario.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mensajes_pendientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero TEXT,
            tipo TEXT,
            mensaje TEXT,
            cuestionario_id INTEGER,
            entregado INTEGER DEFAULT 0,
            fecha_hora TEXT DEFAULT (datetime('now', 'localtime')),
            fecha_entrega TEXT
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
    # Qué zonal es responsable de cada productor. Define el alcance de los números con rol
    # zonal vinculados a ese zonal, la dimensión "zonal" de las consultas e informes, y a
    # quién se avisa cuando un productor reporta un ajuste.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS productores_zonales (
            productor TEXT PRIMARY KEY,
            csg TEXT,
            grupo TEXT,
            zonal TEXT NOT NULL,
            fecha_actualizacion TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    if not conn.execute("SELECT COUNT(*) FROM productores_zonales").fetchone()[0]:
        conn.executemany(
            "INSERT OR IGNORE INTO productores_zonales (productor, csg, grupo, zonal) VALUES (?, ?, ?, ?)",
            ASIGNACION_ZONALES_INICIAL,
        )
    # Ids de los mensajes de WhatsApp ya recibidos. Meta reintenta el webhook si no le
    # respondemos a tiempo; sin esto, una respuesta lenta se procesaba (y contestaba) dos veces.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mensajes_recibidos (
            wamid TEXT PRIMARY KEY,
            fecha_hora TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    # Plantas de un número con perfil planta: ve solo lo que entra a ellas (columna Packing).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS numeros_plantas (
            numero TEXT,
            planta TEXT,
            PRIMARY KEY (numero, planta)
        )
    """)
    # Historial de confirmaciones y ajustes: una fila por cada hecho (cuestionario enviado,
    # confirmación, ajuste, corrección, anulación, reporte espontáneo, resumen a una planta).
    # `cuestionarios` guarda solo el último estado de cada uno; aquí nada se pisa ni se borra.
    # Es la tabla que el bot consulta para los informes (herramienta consultar_confirmaciones).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS confirmaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_hora TEXT DEFAULT (datetime('now', 'localtime')),
            fecha TEXT,
            evento TEXT,
            turno TEXT,
            numero TEXT,
            nombre TEXT,
            perfil TEXT,
            zonal TEXT,
            productores TEXT,
            plantas TEXT,
            kg_previstos REAL,
            detalle TEXT,
            texto_usuario TEXT,
            avisados INTEGER,
            cuestionario_id INTEGER
        )
    """)
    # aviso = lo que se le avisó al equipo: solo lo que la persona informó en ese mensaje.
    if "aviso" not in [fila[1] for fila in conn.execute("PRAGMA table_info(confirmaciones)")]:
        conn.execute("ALTER TABLE confirmaciones ADD COLUMN aviso TEXT")
    columnas_cuest = [fila[1] for fila in conn.execute("PRAGMA table_info(cuestionarios)")]
    # Productores que iban en la tabla de cada cuestionario (JSON): con eso se sabe quién
    # confirmó qué al armar el resumen de cierre de cada planta.
    if "productores" not in columnas_cuest:
        conn.execute("ALTER TABLE cuestionarios ADD COLUMN productores TEXT")
    if not conn.execute("SELECT COUNT(*) FROM confirmaciones").fetchone()[0]:
        # Primera vez: se carga lo que ya había en `cuestionarios` (cada envío y la última
        # respuesta de cada uno; las correcciones anteriores no quedaron guardadas).
        conn.execute("""
            INSERT INTO confirmaciones (fecha_hora, fecha, evento, turno, numero, nombre, perfil, zonal, cuestionario_id)
            SELECT c.fecha_hora_envio, date(c.fecha_hora_envio), 'enviado', c.turno, c.numero,
                   n.nombre, n.rol, n.zonal, c.id
            FROM cuestionarios c LEFT JOIN numeros_permitidos n ON n.numero = c.numero
            WHERE c.turno <> 'espontaneo'
        """)
        conn.execute("""
            INSERT INTO confirmaciones (fecha_hora, fecha, evento, turno, numero, nombre, perfil, zonal,
                                        detalle, texto_usuario, cuestionario_id)
            SELECT c.fecha_hora_respuesta, date(c.fecha_hora_envio),
                   CASE WHEN c.turno = 'espontaneo' THEN 'reporte' ELSE c.estado END, c.turno, c.numero,
                   n.nombre, n.rol, n.zonal, c.ajuste, c.respuesta, c.id
            FROM cuestionarios c LEFT JOIN numeros_permitidos n ON n.numero = c.numero
            WHERE c.estado IN ('confirmado', 'ajustado')
        """)
    columnas_conv = [fila[1] for fila in conn.execute("PRAGMA table_info(conversaciones)")]
    # Herramientas que usó Claude para cada respuesta (nombre, parámetros y un extracto del
    # resultado), para que en la pregunta siguiente sepa qué se consultó ("mándamelo en PDF").
    if "herramientas" not in columnas_conv:
        conn.execute("ALTER TABLE conversaciones ADD COLUMN herramientas TEXT")
    columnas = [fila[1] for fila in conn.execute("PRAGMA table_info(numeros_permitidos)")]
    # Zonal (de productores_zonales) al que corresponde un número con rol zonal: ve los
    # productores de ese zonal sin tener que asignárselos uno por uno.
    if "zonal" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN zonal TEXT")
    if "rol" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN rol TEXT DEFAULT 'productor'")
    if "productor" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN productor TEXT")
    # Gerencia y EAS solo reciben envíos automáticos si se les activa explícitamente
    # ("reciben notificaciones sólo si lo necesitan").
    if "recibe_notificaciones" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN recibe_notificaciones INTEGER DEFAULT 0")
    # Permiso explícito para que un zonal o productor vea TODOS los fundos sin dejar de
    # tener su rol. Es deliberado y se quita por número; distinto de "no tiene fundos
    # asignados", que significa que no debe ver nada.
    if "acceso_total" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN acceso_total INTEGER DEFAULT 0")
    # Qué ve de embarques un EAS o gerencia: 0 = nada, 1 = embarques sin FOB, 2 = también el
    # FOB. Vacío = todo (lo que veían antes de existir la opción).
    if "ver_embarques" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN ver_embarques INTEGER")
    # Qué bases lee un usuario: 'ambas', 'cosecha' o 'packing'. Vacío = solo cosecha: Packing se
    # habilita usuario por usuario (ver bases_de).
    if "ver_bases" not in columnas:
        conn.execute("ALTER TABLE numeros_permitidos ADD COLUMN ver_bases TEXT")
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
#   zonal     - responsable de sus fundos: consulta solo los suyos, responde los cuestionarios
#               y AUTORIZA los ajustes que piden sus productores.
#   productor - consulta solo sus fundos y PIDE ajustes de cosecha; su solicitud pasa por el
#               zonal antes de darse por buena.
#   planta    - consulta SOLO lo que entra a su(s) planta(s) (columna Packing) y recibe cada día,
#               al cierre, el resumen de lo que confirmaron o ajustaron los zonales para su
#               planta. No responde cuestionarios, no reporta ajustes y no ve embarques.
# PENDIENTE: el circuito de autorización (el productor pide y el zonal aprueba) todavía no
# está implementado. Hoy cualquier ajuste reportado se registra directo y se avisa al EAS.
# Mientras tanto no hay números con perfil productor, así que no se usa esa ruta.
ROLES_VALIDOS = ("admin", "gerencia", "eas", "zonal", "productor", "planta")
# Roles sin restricción de fundos (ven la información de todos los productores).
ROLES_VEN_TODO = ("admin", "gerencia", "eas")
# Roles cuyo acceso se limita a los fundos que tengan asignados.
ROLES_ACOTADOS = ("zonal", "productor")
ROL_POR_DEFECTO = "productor"

def agregar_numero_permitido(numero, nombre=None, rol=None, productor=None, recibe_notificaciones=None,
                             acceso_total=None, zonal=None):
    """Inserta o actualiza un número. Los campos que vengan en None no se tocan, para no
    pisar lo existente al re-agregar un número solo para cambiarle el nombre. `productor`
    se agrega como un fundo más (un número puede tener varios). `zonal` vincula el número a
    un zonal de productores_zonales; "" lo desvincula."""
    conn = sqlite3.connect(DB_LOCAL_PATH)
    num = normalizar_numero(numero)
    existe = conn.execute("SELECT 1 FROM numeros_permitidos WHERE numero = ?", (num,)).fetchone()
    if existe:
        conn.execute(
            "UPDATE numeros_permitidos SET nombre = COALESCE(?, nombre), rol = COALESCE(?, rol), "
            "recibe_notificaciones = COALESCE(?, recibe_notificaciones), "
            "acceso_total = COALESCE(?, acceso_total) WHERE numero = ?",
            (nombre, rol, recibe_notificaciones, acceso_total, num)
        )
    else:
        conn.execute(
            "INSERT INTO numeros_permitidos (numero, nombre, rol, recibe_notificaciones, acceso_total) "
            "VALUES (?, ?, ?, ?, ?)",
            (num, nombre, rol or ROL_POR_DEFECTO, recibe_notificaciones or 0, acceso_total or 0)
        )
    if zonal is not None:
        conn.execute("UPDATE numeros_permitidos SET zonal = ? WHERE numero = ?", (zonal or None, num))
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

def asignar_fundos(numero, fundos, reemplazar=False, revocar_acceso_total=True):
    """Asigna fundos a un número. Con reemplazar=True deja exactamente los indicados.

    Asignarle fundos a alguien significa acotarlo a esos fundos, así que se le retira el
    acceso total: si no, seguiría viendo todo y quien lo asignó creería que quedó
    restringido. Con revocar_acceso_total=False se conserva (para cuando en la misma
    llamada se pidió acceso_total explícitamente)."""
    num = normalizar_numero(numero)
    conn = sqlite3.connect(DB_LOCAL_PATH)
    if reemplazar:
        conn.execute("DELETE FROM numeros_fundos WHERE numero = ?", (num,))
    asignados = 0
    for fundo in fundos:
        fundo = (fundo or "").strip()
        if fundo:
            conn.execute(
                "INSERT OR IGNORE INTO numeros_fundos (numero, fundo) VALUES (?, ?)", (num, fundo)
            )
            asignados += 1
    revocado = False
    if asignados and revocar_acceso_total:
        cursor = conn.execute(
            "UPDATE numeros_permitidos SET acceso_total = 0 WHERE numero = ? AND acceso_total = 1",
            (num,)
        )
        revocado = cursor.rowcount > 0
    conn.commit()
    conn.close()
    if revocado:
        logger.info(f"{num}: se le retiró el acceso total al asignarle {asignados} fundo(s)")
    return revocado

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

def tiene_acceso_total(numero):
    """¿Se le dio permiso explícito de ver todos los fundos pese a tener un rol acotado?"""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT acceso_total FROM numeros_permitidos WHERE numero = ?",
            (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return bool(fila and fila[0])
    except Exception as e:
        logger.error(f"Error verificando acceso total: {str(e)}")
        return False

def alcance_de(numero):
    """Qué fundos puede consultar ese número:
       None      -> sin restricción (admin, gerencia, eas, o acceso_total explícito)
       []        -> rol acotado SIN fundos asignados: no debe ver nada
       [fundos]  -> rol acotado, solo esos fundos (los asignados a mano más los de su zonal)
    """
    rol = rol_de(numero)
    if rol is None or rol in ROLES_VEN_TODO:
        return None
    if rol == "planta":
        # Va antes del acceso total: una planta ve su planta y nada más.
        return AlcancePlanta(plantas_de(numero))
    if tiene_acceso_total(numero):
        return None
    return fundos_efectivos(numero)

# ============================================================================
# ZONALES: qué zonal es responsable de cada productor
# ============================================================================

ZONAL_SIN_ASIGNAR = "Sin zonal"

def _normalizar_nombre(texto):
    """Mayúsculas, sin tildes y con un solo espacio entre palabras, para comparar nombres
    que vienen escritos distinto (la planilla dice "VICTORIA CIA." y la base "VICTORIA  CIA.")."""
    sin_tildes = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode()
    return " ".join(sin_tildes.upper().split())

def _clave_fundo(texto):
    """Clave para reconocer el MISMO fundo escrito de varias formas. Los nombres se cargan a
    mano en cada base, así que varían en tildes, puntos y abreviaturas: 'AGRÍCOLA EL CARMELO
    LTDA' y 'AGRICOLA EL CARMELO LTDA', o 'FRUTICOLA TANTEHUE LIMITADA - SANTA RAQUEL' y
    'FRUTICOLA TANTEHUE LTDA. - SANTA RAQUEL'.

    Solo quita diferencias de ESCRITURA: no acerca nombres parecidos. Eso importa porque
    'AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA' y '... - LOS SUSPIROS' son fundos distintos y
    de zonales distintos, y deben seguir separados."""
    base = _normalizar_nombre(texto)
    base = re.sub(r"\bLIMITADA\b", "LTDA", base)
    base = re.sub(r"\bSOCIEDAD\b", "SOC", base)
    base = re.sub(r"[^A-Z0-9]+", " ", base)
    return " ".join(base.split())

def asignacion_zonales():
    """{productor: zonal} tal como está en productores_zonales."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        filas = conn.execute("SELECT productor, zonal FROM productores_zonales").fetchall()
        conn.close()
        return {p: z for p, z in filas}
    except Exception as e:
        logger.error(f"Error leyendo la asignación de zonales: {str(e)}")
        return {}

def detalle_zonales():
    """Filas completas de la asignación (productor, csg, grupo, zonal), ordenadas."""
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.row_factory = sqlite3.Row
    filas = [dict(r) for r in conn.execute(
        "SELECT productor, csg, grupo, zonal, fecha_actualizacion FROM productores_zonales "
        "ORDER BY zonal, productor"
    )]
    conn.close()
    return filas

def zonales_conocidos():
    return sorted(set(asignacion_zonales().values()), key=_normalizar_nombre)

def resolver_zonal(texto):
    """Nombre exacto del zonal a partir de lo que escriba alguien ("joaquin", "jose luis
    castro", "Pino"). None si no calza con ninguno o si calza con más de uno."""
    buscado = _normalizar_nombre(texto)
    if not buscado:
        return None
    zonales = zonales_conocidos()
    exactos = [z for z in zonales if _normalizar_nombre(z) == buscado]
    if exactos:
        return exactos[0]
    parciales = [z for z in zonales if buscado in _normalizar_nombre(z)]
    return parciales[0] if len(parciales) == 1 else None

def productores_de_zonal(zonal):
    real = resolver_zonal(zonal)
    if not real:
        return []
    return sorted(p for p, z in asignacion_zonales().items() if z == real)

def zonal_de_numero(numero):
    """Zonal al que está vinculado ese número (o None)."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT zonal FROM numeros_permitidos WHERE numero = ?", (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return fila[0] if fila and fila[0] else None
    except Exception as e:
        logger.error(f"Error obteniendo zonal del número: {str(e)}")
        return None

def fundos_efectivos(numero):
    """Fundos que ve un número: los asignados a mano más los productores de su zonal. Se
    calcula en cada consulta, así que cambiar la asignación de zonales lo actualiza solo."""
    fundos = list(fundos_de(numero))
    zonal = zonal_de_numero(numero)
    if zonal:
        fundos += [p for p in productores_de_zonal(zonal) if p not in fundos]
    return fundos

class AlcancePlanta(list):
    """Alcance de un usuario con perfil planta: las plantas (columna Packing) que puede ver.
    Es una lista, como la de fundos, para que pase por los mismos caminos (vacía = no ve
    nada); los filtros la aplican sobre Packing en vez de Productor."""

def plantas_de(numero):
    """Plantas asignadas a ese número (perfil planta)."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        filas = conn.execute(
            "SELECT planta FROM numeros_plantas WHERE numero = ? ORDER BY planta",
            (normalizar_numero(numero),)
        ).fetchall()
        conn.close()
        return [f[0] for f in filas]
    except Exception as e:
        logger.error(f"Error obteniendo plantas: {str(e)}")
        return []

def asignar_plantas(numero, plantas):
    """Deja exactamente esas plantas al número."""
    num = normalizar_numero(numero)
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.execute("DELETE FROM numeros_plantas WHERE numero = ?", (num,))
    for planta in plantas:
        conn.execute("INSERT OR IGNORE INTO numeros_plantas (numero, planta) VALUES (?, ?)", (num, planta))
    conn.commit()
    conn.close()

def _productores_conocidos_normalizados():
    return {_normalizar_nombre(p): p for p in PRODUCTORES_CONOCIDOS}

def sincronizar_nombres_zonales():
    """Deja cada productor de la asignación con el nombre EXACTO que tiene en la base, para
    que los filtros por igualdad calcen aunque la planilla tenga otro espaciado o tildes."""
    conocidos = _productores_conocidos_normalizados()
    if not conocidos:
        return
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        corregidos = 0
        for (productor,) in conn.execute("SELECT productor FROM productores_zonales").fetchall():
            real = conocidos.get(_normalizar_nombre(productor))
            if real and real != productor:
                # OR IGNORE: si ya hay una fila con el nombre real, se respeta esa.
                conn.execute(
                    "UPDATE OR IGNORE productores_zonales SET productor = ? WHERE productor = ?",
                    (real, productor),
                )
                corregidos += 1
        conn.commit()
        conn.close()
        if corregidos:
            logger.info(f"Zonales: {corregidos} nombre(s) de productor ajustados al de la base")
    except Exception as e:
        logger.error(f"Error sincronizando nombres de zonales: {str(e)}")

def _literal_sql(texto):
    """Literal de texto para SQL Server (N'...'), escapando las comillas. Se usa solo con
    nombres que vienen de nuestras propias tablas, no con texto del usuario."""
    return "N'" + str(texto).replace("'", "''") + "'"

def _sql_expr_zonal(columna="Productor"):
    """Expresión SQL que traduce cada productor a su zonal. Va con literales y no con
    parámetros porque se repite tal cual en el SELECT y en el GROUP BY, y SQL Server solo
    los reconoce como la misma expresión si el texto es idéntico."""
    por_zonal = {}
    for productor, zonal in asignacion_zonales().items():
        # Se incluyen las otras escrituras del mismo fundo: si no, esas filas salen
        # "Sin zonal" aunque el productor sí tenga zonal asignado.
        for nombre in (variantes_de_fundo(productor) or [productor]):
            por_zonal.setdefault(zonal, []).append(nombre)
    if not por_zonal:
        return _literal_sql(ZONAL_SIN_ASIGNAR)
    casos = " ".join(
        f"WHEN {columna} IN ({', '.join(_literal_sql(p) for p in sorted(productores))}) "
        f"THEN {_literal_sql(zonal)}"
        for zonal, productores in sorted(por_zonal.items())
    )
    return f"(CASE {casos} ELSE {_literal_sql(ZONAL_SIN_ASIGNAR)} END)"

def _sql_fundos(fundos, columna="Productor", literales=False):
    """Condición que limita a esos fundos, como (sql, params). Un fundo que es el nombre
    EXACTO de un productor de la base se compara por igualdad; si no, por coincidencia
    parcial (permite asignar "CARMELO" a mano). La igualdad importa: con LIKE, asignar
    "AGROINDUSTRIAL Y COMERCIAL SUPERFRUIT LTDA." también abría "... - LOS SUSPIROS",
    que es de otro zonal."""
    conocidos = _productores_conocidos_normalizados()
    exactos, parciales = [], []
    for fundo in fundos:
        fundo = (fundo or "").strip()
        if not fundo:
            continue
        # Todas las formas en que está escrito ese mismo fundo (tildes, puntos, LTDA/LIMITADA).
        variantes = variantes_de_fundo(fundo)
        if variantes:
            exactos.extend(variantes)
            continue
        real = conocidos.get(_normalizar_nombre(fundo))
        if real:
            exactos.append(real)
        else:
            parciales.append(fundo)
    exactos = sorted(set(exactos))
    partes, params = [], []
    if exactos:
        if literales:
            partes.append(f"{columna} IN ({', '.join(_literal_sql(p) for p in exactos)})")
        else:
            partes.append(f"{columna} IN ({', '.join('?' * len(exactos))})")
            params.extend(exactos)
    for parcial in parciales:
        if literales:
            partes.append(f"{columna} LIKE {_literal_sql('%' + parcial + '%')}")
        else:
            partes.append(f"{columna} LIKE ?")
            params.append(f"%{parcial}%")
    if not partes:
        return "1 = 0", []
    return "(" + " OR ".join(partes) + ")", params

def _sql_plantas(plantas, columna="Packing", literales=False):
    """Condición que limita a esas plantas (nombre de Packing, sin espacios de relleno)."""
    plantas = [p for p in plantas if p]
    if not plantas:
        return "1 = 0", []
    campo = f"LTRIM(RTRIM({columna}))"
    if literales:
        return f"{campo} IN ({', '.join(_literal_sql(p) for p in plantas)})", []
    return f"{campo} IN ({', '.join('?' * len(plantas))})", list(plantas)

def _sql_alcance(alcance):
    """Condición SQL que limita una consulta a los fundos permitidos, como (sql, params).
    Es la barrera que impide que un zonal o productor vea fundos que no son suyos."""
    if alcance is None:
        return None, []
    if not alcance:
        # Rol acotado sin fundos asignados: no puede ver ninguna fila.
        return "1 = 0", []
    if isinstance(alcance, AlcancePlanta):
        return _sql_plantas(alcance)
    return _sql_fundos(alcance)

MENSAJE_SIN_PLANTA = (
    "Todavía no tienes una planta asignada, así que no puedo mostrarte información. "
    "Pídele a quien administra el asistente que te asigne tu planta."
)

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
        f"SELECT numero, nombre, rol, recibe_notificaciones, acceso_total, zonal FROM numeros_permitidos "
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
        fila["fundos"] = fundos_efectivos(fila["numero"])
        fila["acceso_total"] = bool(fila["acceso_total"])
        resultado.append(fila)
    return resultado

def quitar_numero_permitido(numero):
    conn = sqlite3.connect(DB_LOCAL_PATH)
    cursor = conn.execute("DELETE FROM numeros_permitidos WHERE numero = ?", (normalizar_numero(numero),))
    eliminado = cursor.rowcount > 0
    conn.execute("DELETE FROM numeros_plantas WHERE numero = ?", (normalizar_numero(numero),))
    conn.commit()
    conn.close()
    return eliminado

def listar_numeros_permitidos():
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT numero, nombre, rol, recibe_notificaciones, acceso_total, zonal, fecha_agregado "
        "FROM numeros_permitidos ORDER BY fecha_agregado DESC"
    )
    filas = [dict(row) for row in cursor.fetchall()]
    for fila in filas:
        fila["fundos"] = fundos_de(fila["numero"])
        if fila["zonal"]:
            fila["fundos_de_su_zonal"] = len(productores_de_zonal(fila["zonal"]))
        fila["ve_todo"] = (fila["rol"] or ROL_POR_DEFECTO) in ROLES_VEN_TODO or bool(fila["acceso_total"])
        fila["plantas"] = plantas_de(fila["numero"])
        fila["embarques"] = NOMBRE_NIVEL_EMBARQUES[nivel_embarques(fila["numero"])]
        fila["bases"] = NOMBRE_BASES[bases_de(fila["numero"])]
    conn.close()
    return filas

def guardar_conversacion(numero_sender, tipo_mensaje, mensaje, respuesta, herramientas=None):
    """herramientas: rondas de herramientas que usó Claude para esa respuesta (ver
    procesar_mensaje), guardadas como JSON para reconstruir el contexto después."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute(
            "INSERT INTO conversaciones (numero_sender, tipo_mensaje, mensaje, respuesta, herramientas) "
            "VALUES (?, ?, ?, ?, ?)",
            (numero_sender, tipo_mensaje, mensaje, respuesta,
             json.dumps(herramientas, ensure_ascii=False, default=str) if herramientas else None)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error guardando conversación: {str(e)}")

# Memoria del chat: como en un chat de IA, la persona puede retomar lo que conversó hoy
# ("lo mismo pero por zonal", "mándamelo en PDF") sin repetir todo. Antes eran 6 mensajes
# de la última hora y las preguntas de seguimiento se perdían con facilidad.
LIMITE_HISTORIAL_MENSAJES = int(os.getenv("LIMITE_HISTORIAL_MENSAJES", "20"))
LIMITE_HISTORIAL_MINUTOS = int(os.getenv("LIMITE_HISTORIAL_MINUTOS", str(24 * 60)))

def obtener_historial_conversacion(numero_sender):
    """
    Trae los últimos mensajes recientes de ESE número (misma sesión de chat) para darle
    contexto a Claude y que pueda entender preguntas de seguimiento (ej. "y de Lapins?",
    "conviértelo a bins"). Solo los de las últimas LIMITE_HISTORIAL_MINUTOS, para no
    arrastrar contexto viejo.
    """
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """SELECT mensaje, respuesta, herramientas FROM conversaciones
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

# Modo mantención: si existe este archivo, el bot no abre ninguna conexión a SQL Server, responde
# con un aviso y no hace los envíos programados. Se activa y desactiva sin reiniciar, con
# /admin/base?estado=desconectar|conectar (o creando/borrando el archivo).
ARCHIVO_BASE_DESCONECTADA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "BASE_DESCONECTADA")
MENSAJE_BASE_DESCONECTADA = (
    "La base de datos de cosecha y embarques está en mantención por un momento, así que ahora no "
    "puedo consultar cifras ni armar informes. Vuelve a escribirme en un rato."
)

def base_desconectada():
    return os.path.exists(ARCHIVO_BASE_DESCONECTADA)

def conectar_sql():
    """Conecta a SQL Server"""
    if base_desconectada():
        logger.warning("Base desconectada (modo mantención): no se abre conexión a SQL Server")
        return None
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

# La Trisemanal (estimación de cosecha por día) sale SIEMPRE de la vista
# [PKG-Cos-Trisemanal-Vigente], no de la tabla [PKG-Cos-Trisemanal]: la vista trae un solo valor
# por día, el de la trisemanal más reciente (la columna Trisemanal dice de cuál viene), así que no
# se filtra ni se agrupa por horizonte. Recepcion_Consolidada solo trae los primeros días de cada
# trisemanal (el 02-11-2026: 2 días contra 14 de la vista), así que sus filas 'Trisemanal' se
# reemplazan por las de la vista. Todas las consultas de cosecha leen esta unión, que conserva las
# columnas de Recepcion_Consolidada y agrega CajasEq y Trisemanal (solo en las filas Trisemanal).
RECEPCION_SQL = (
    "(SELECT Temporada, [Base Origen], Especie, Variedad, [Variedad Agronomica], Productor, Grupo, Packing, "
    "Fecha, Semana, Envase, Bultos, KgsRecepcionados, Terminado, Productor_R, Grupo_R, tipo_despacho, "
    "CAST(NULL AS float) AS CajasEq, CAST(NULL AS float) AS Trisemanal "
    f"FROM [Recepcion_Consolidada] WHERE ISNULL([Base Origen], '') <> '{BASE_ORIGEN_TRISEMANAL}' "
    "UNION ALL "
    f"SELECT Temporada, N'{BASE_ORIGEN_TRISEMANAL}', Especie, Variedad, Variedad, Productor, Grupo, Packing, "
    "[Fecha Cosecha], Semana, Envase, Bultos, KgsRecepcionados, 'NO', Productor, Grupo, NULL, "
    "[Cajas Eq], Trisemanal FROM [PKG-Cos-Trisemanal-Vigente])"
)

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

# Factores de conversión por especie y envase: planilla de parámetros que entregó Edson el
# 03-11-2026. None = en blanco en la planilla (no aplica o sin dato: no se inventa). Por fila:
# (Especie, Envase, Defecto, kg por caja equivalente, % exportable, % comercial, kg por envase,
#  cajas equivalentes por envase, envases por bin, cajas equivalentes por pallet, kg por bin).
# Defecto = 1 en la fila que se usa si no importa el envase (PEAR y PLUM tienen dos).
CONVERSIONES_ESPECIE = [
    ("APRICOT", "TOTES", 1, 6.0, 0.82, 0.18, 9.8, 1.3, 24, 120, 240),
    ("BLUEBERRY", "BANDEJA", 1, 1.5, 1.00, 0.00, 3.5, None, None, None, None),
    ("CHERRY", "TOTES", 1, 5.0, 0.83, 0.17, 8.8, 1.3, 24, 184, 228),
    ("GRAPE", "CAJA EQ", 1, 8.2, 0.92, 0.08, 8.2, 1.0, None, 108, None),
    ("KIWI", "BINS", 1, 10.0, 0.88, 0.12, 429.0, 36.0, 1, 120, 450),
    ("MANDARIN", "BINS", 1, 15.0, 0.80, 0.20, 388.0, 22.0, 1, 72, 350),
    ("NECTARINE", "BINS", 1, 8.0, 0.86, 0.14, 405.0, 40.0, 1, 120, 380),
    ("PEACH", "BINS", 1, 8.0, 0.84, 0.16, 383.0, 40.0, 1, 120, 380),
    ("FLAT PEACH", "TOTES", 1, 8.0, 0.84, 0.16, 9.7, 0.95, 24, None, 233),
    ("PEAR", "BINS", 1, 18.0, 0.83, 0.17, 444.0, 21.0, 1, 64, 450),
    ("PEAR", "CAJA EQ", 0, 18.0, 1.00, 0.00, 10.9, 1.0, 24, None, 450),
    ("PLUM", "BINS", 1, 7.0, 0.81, 0.19, 433.0, 45.0, 1, 120, 400),
    ("PLUM", "TOTES", 0, 7.0, 0.81, 0.19, 10.6, None, 24, 120, 400),
    ("SUGAR PLUM", "TOTES", 1, 7.0, 0.88, 0.12, 10.2, 1.2, 24, 120, 245),
    ("ORANGE", "BINS", 1, 15.0, 0.85, 0.15, 400.0, 22.0, 1, None, None),
]
COLUMNAS_CONVERSIONES = ("Especie", "Envase", "Defecto", "PesoCajaEqKg", "PctExportable", "PctComercial",
                         "PesoEnvaseKg", "CajasEqPorEnvase", "EnvasesPorBin", "CajasEqPorPallet", "PesoBinKg")

def _cte_conversiones():
    """La planilla como tabla `conversiones` de consultar_sql (igual para todos los perfiles)."""
    def valor(x):
        if x is None:
            return "NULL"
        if isinstance(x, str):
            return "N'" + x.replace("'", "''") + "'"
        return f"{float(x)!r}E0"
    filas = ", ".join("(" + ", ".join(valor(x) for x in fila) + ")" for fila in CONVERSIONES_ESPECIE)
    return f"conversiones AS (SELECT * FROM (VALUES {filas}) AS v({', '.join(COLUMNAS_CONVERSIONES)}))"

def _texto_conversiones():
    """La planilla para las instrucciones de Claude, una línea por especie y envase."""
    def num(x, pct=False):
        if x is None:
            return "—"
        if pct:
            return f"{round(x * 100)}%"
        return f"{x:g}".replace(".", ",")
    lineas = ["Especie (envase): kg/caja eq · % exportable · % comercial · kg/envase · cajas eq/envase · "
              "envases/bin · cajas eq/pallet · kg/bin"]
    for (especie, envase, _defecto, caja, exp, com, envase_kg, cajas_env, env_bin, cajas_pallet, kg_bin) in CONVERSIONES_ESPECIE:
        lineas.append(f"{especie} ({envase}): {num(caja)} · {num(exp, True)} · {num(com, True)} · {num(envase_kg)} · "
                      f"{num(cajas_env)} · {num(env_bin)} · {num(cajas_pallet)} · {num(kg_bin)}")
    return "\n".join(lineas)

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
        FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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
        FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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
            f"""SELECT SUM(KgsRecepcionados) FROM {RECEPCION_SQL} AS Recepcion_Consolidada
               WHERE Variedad LIKE ? AND [Base Origen] = ?
               AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE)){filtro_alc}""",
            [f"%{variedad}%", base, fecha] + params_alc
        )
        fila = cursor.fetchone()
        estimado = fila[0] if fila and fila[0] else 0

        cursor.execute(
            f"""SELECT SUM(KgsRecepcionados) FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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
        FROM {RECEPCION_SQL} AS Recepcion_Consolidada
        WHERE Productor LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM {RECEPCION_SQL} AS Recepcion_Consolidada WHERE Fecha <= GETDATE())
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
        FROM {RECEPCION_SQL} AS Recepcion_Consolidada
        WHERE Packing LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM {RECEPCION_SQL} AS Recepcion_Consolidada WHERE Fecha <= GETDATE())
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
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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
    # La columna no existe en la base: sale de productores_zonales (ver _columna_sql).
    "zonal": None,
}
DIMENSIONES_ETIQUETA = {
    "especie": "Especie",
    "variedad": "Variedad",
    "productor": "Productor",
    "packing": "Planta",
    "grupo": "Grupo",
    "fecha": "Fecha",
    "semana": "Semana",
    "zonal": "Zonal",
}
MAX_FILAS_FLEXIBLE = 60

def _columna_sql(dimension):
    """Expresión SQL de una dimensión. El zonal se arma en cada consulta desde la tabla de
    asignación, para que un cambio en /admin/zonales se vea de inmediato."""
    if dimension == "zonal":
        return _sql_expr_zonal()
    return DIMENSIONES_SQL[dimension]

def _filtro_zonal(zonal):
    """(sql, params, nombre_real, error) para filtrar por los productores de un zonal. Si el
    zonal no existe, sql es None y error trae el texto para el usuario."""
    real = resolver_zonal(zonal)
    productores = productores_de_zonal(real) if real else []
    if not productores:
        return None, [], None, (
            f"No encontré al zonal '{zonal}'. Los zonales son: {_enumerar(zonales_conocidos())}."
        )
    return f"Productor IN ({', '.join('?' * len(productores))})", productores, real, None

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
        elif d in ("productor", "packing", "zonal"):
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
        lineas.append(
            f"\n(mostrando {MAX_FILAS_FLEXIBLE} de {len(claves_ordenadas)} filas; los totales "
            f"sí incluyen todo. Si lo quieres completo, pídemelo en Excel o PDF)"
        )

    lineas.append(
        f"\n📦 Total general: {etiqueta_estimado} {formatear_kg(tot_est)} {unidad}, real {formatear_kg(tot_real)} {unidad}"
    )
    return "\n".join(lineas)

def obtener_cosecha_flexible(agrupar_por, fecha_inicio=None, fecha_fin=None, especie=None, variedad=None,
                              productor=None, packing=None, grupo=None, envase=None, temporada=None,
                              fuente_estimado=None, kg_por_caja_eq=None, alcance=None,
                              formato_archivo=None, numero_destino=None, zonal=None,
                              grafico=None, resumen=None):
    """
    Consulta genérica: agrupa por las dimensiones exactas que se pidan (cualquier combinación
    de especie/variedad/productor/packing/grupo/zonal/fecha, o ninguna si se pide solo el
    total), sumando estimado y real (Recepción Planta) o Bultos si se da envase. Por defecto el
    "estimado" es Estim Primavera; fuente_estimado puede forzar 'trisemanal' o 'invierno'
    explícitamente. A diferencia de las demás consultas, si no se da ningún periodo (ni fechas
    ni temporada) NO asume nada: pide que se aclare el periodo.
    Con formato_archivo arma un informe (PDF o Excel) con totales, gráfico y la tabla
    completa; grafico y resumen permiten pedir otro gráfico o agregar un comentario.
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
        if zonal:
            sql_zonal, params_zonal, zonal_real, error_zonal = _filtro_zonal(zonal)
            if error_zonal:
                conn.close()
                return error_zonal
            condiciones.append(sql_zonal)
            params.extend(params_zonal)
            filtro_desc += f" del zonal {zonal_real}"

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
        columnas_sql = ", ".join(_columna_sql(d) for d in dimensiones)
        select_cols = f"{columnas_sql}, " if columnas_sql else ""
        group_cols = f"{columnas_sql}, " if columnas_sql else ""
        query = f"""
            SELECT {select_cols}[Base Origen], SUM({columna_suma}) as total
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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

        if formato_archivo:
            # El archivo lleva TODAS las filas: no tiene el tope de la tabla del chat.
            informe = informe_cosecha_flexible(
                filas, dimensiones, unidad, base_estimado, fecha_inicio, fecha_fin,
                filtro_desc, grafico=grafico, resumen=resumen,
            )
            return enviar_informe(numero_destino, formato_archivo, informe)

        resultado = formatear_cosecha_flexible(filas, dimensiones, fecha_inicio, fecha_fin, filtro_desc, unidad, base_estimado)
        if not resultado:
            return f"No hay datos registrados{filtro_desc} entre {fecha_inicio} y {fecha_fin}"
        return resultado
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_flexible: {str(e)}")
        return f"Error al consultar: {str(e)}"

def _filas_para_archivo(filas, dimensiones, unidad, base_estimado):
    """Convierte el resultado crudo de la consulta flexible en encabezados + filas planas,
    una fila por combinación, con una columna por fuente. Sin topes: el archivo lleva todo."""
    etiqueta_est = ETIQUETA_FUENTE.get(base_estimado, "Estimado")
    datos = {}
    for fila in filas:
        clave = tuple(fila[:len(dimensiones)])
        origen = fila[len(dimensiones)]
        total = fila[-1] or 0
        acum = datos.setdefault(clave, {"est": 0, "real": 0})
        acum["real" if origen == BASE_ORIGEN_REAL else "est"] += total

    encabezados = [DIMENSIONES_ETIQUETA.get(d, d.title()) for d in dimensiones]
    encabezados += [f"{etiqueta_est} ({unidad})", f"Real ({unidad})",
                    f"Diferencia ({unidad})", "Desviación %"]

    def ordenar(clave):
        return [orden_semana(v) if d == "semana" and v is not None else str(v)
                for v, d in zip(clave, dimensiones)]

    filas_salida = []
    for clave in sorted(datos, key=ordenar):
        est = datos[clave]["est"]
        real = datos[clave]["real"]
        valores = []
        for v, d in zip(clave, dimensiones):
            if d == "fecha":
                valores.append(_fecha_str(v))
            elif d == "semana":
                valores.append(etiqueta_semana(v) if v is not None else "-")
            elif d == "especie":
                valores.append(traducir_especie(v))
            elif d == "grupo":
                valores.append(_formatear_grupo(v))
            else:
                valores.append(str(v).strip() if v is not None else "-")
        desviacion = round((real - est) / est * 100, 1) if est else None
        # Sin redondear: el informe muestra enteros, pero suma los valores exactos para que
        # su TOTAL calce al kilo con el que da el bot en el chat.
        filas_salida.append(valores + [est, real, real - est,
                                       desviacion if desviacion is not None else ""])
    return encabezados, filas_salida

FUENTES_PROYECCION = {
    "precosecha": (BASE_ORIGEN_ESTIM_PRIMAVERA, "Precosecha", "Prec."),
    "trisemanal": (BASE_ORIGEN_TRISEMANAL, "Trisemanal", "Tri."),
    "real": (BASE_ORIGEN_REAL, "Real", "Real"),
    "invierno": (BASE_ORIGEN_ESTIM_INVIERNO, "Estim. Inv", "Inv."),
}
DIMENSIONES_PROYECCION_POR_DEFECTO = ["productor", "especie", "variedad", "packing"]

def obtener_proyeccion(fecha_inicio, fecha_fin, dimensiones, fuentes, filtros=None, alcance=None,
                       productores=None):
    """Filas de la proyección: una por combinación de dimensiones, semana y fuente.
    Devuelve (clave_dimensiones, semana, origen, kg). productores acota a esa lista exacta
    (lo usa el filtro por zonal)."""
    conn = conectar_sql()
    if not conn:
        return None
    try:
        origenes = [FUENTES_PROYECCION[f][0] for f in fuentes]
        condiciones = [
            "[Base Origen] IN ({})".format(",".join("?" * len(origenes))),
            "CAST(Fecha AS DATE) BETWEEN ? AND ?",
            "KgsRecepcionados IS NOT NULL",
        ]
        params = list(origenes) + [fecha_inicio.strftime("%Y-%m-%d"), fecha_fin.strftime("%Y-%m-%d")]
        for columna, valor in (filtros or {}).items():
            if valor:
                condiciones.append(f"{columna} LIKE ?")
                params.append(f"%{valor}%")
        if productores:
            condiciones.append(f"Productor IN ({', '.join('?' * len(productores))})")
            params.extend(productores)
        sql_alc, params_alc = _sql_alcance(alcance)
        if sql_alc:
            condiciones.append(sql_alc)
            params.extend(params_alc)
        columnas = ", ".join(_columna_sql(d) for d in dimensiones)
        select_dim = f"{columnas}, " if columnas else ""
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT {select_dim}CAST(Semana AS INT) AS semana, [Base Origen],
                   SUM(KgsRecepcionados) AS kg
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
            WHERE {" AND ".join(condiciones)}
            GROUP BY {select_dim}CAST(Semana AS INT), [Base Origen]
            HAVING SUM(KgsRecepcionados) > 0
        """, params)
        return cursor.fetchall()
    finally:
        conn.close()

def armar_tabla_proyeccion(filas, dimensiones, fuentes):
    """Tabla cruzada: las dimensiones como filas y las SEMANAS como columnas, con una
    columna por fuente dentro de cada semana cuando se pide más de una. Agrega la columna
    Total por fila y la fila TOTAL al pie."""
    n_dim = len(dimensiones)
    datos, semanas = {}, set()
    for fila in filas:
        clave = tuple(str(v).strip() if v is not None else "-" for v in fila[:n_dim])
        semana, origen, kg = fila[n_dim], fila[n_dim + 1], fila[n_dim + 2] or 0
        semanas.add(semana)
        datos.setdefault(clave, {})[(semana, origen)] = \
            datos.setdefault(clave, {}).get((semana, origen), 0) + kg

    semanas = sorted(semanas, key=orden_semana)
    varias = len(fuentes) > 1
    encabezados = [DIMENSIONES_ETIQUETA.get(d, d.title()) for d in dimensiones]
    columnas = []  # (semana, origen) en el orden de las columnas
    for semana in semanas:
        for f in fuentes:
            origen, largo, corto = FUENTES_PROYECCION[f]
            columnas.append((semana, origen))
            encabezados.append(f"S{etiqueta_semana(semana)} {corto}" if varias else f"S{etiqueta_semana(semana)}")
    encabezados.append("Total")

    def ordenar(clave):
        return [v.upper() for v in clave]

    filas_salida, totales = [], [0] * (len(columnas) + 1)
    for clave in sorted(datos, key=ordenar):
        valores, suma = [], 0
        for i, col in enumerate(columnas):
            # Sin redondear aquí (el informe muestra enteros): así el TOTAL es la suma exacta.
            kg = datos[clave].get(col, 0)
            valores.append(kg if kg else "")
            suma += kg
            totales[i] += kg
        totales[-1] += suma
        etiquetas = []
        for v, d in zip(clave, dimensiones):
            etiquetas.append(traducir_especie(v) if d == "especie"
                             else _formatear_grupo(v) if d == "grupo" else v)
        filas_salida.append(etiquetas + valores + [suma])
    if filas_salida:
        filas_salida.append(["TOTAL"] + [""] * (n_dim - 1) +
                            [t if t else "" for t in totales])
    return encabezados, filas_salida

def _enumerar(valores):
    """Une una lista en castellano: 'a', 'a y b', 'a, b y c'."""
    items = [str(v) for v in valores]
    if len(items) <= 1:
        return items[0] if items else ""
    return f"{', '.join(items[:-1])} y {items[-1]}"

def generar_informe_proyeccion(numero_destino=None, semanas=None, fecha_inicio=None,
                               fuentes=None, agrupar_por=None, formato="pdf",
                               especie=None, variedad=None, productor=None, packing=None,
                               grupo=None, alcance=None, zonal=None, grafico=None, resumen=None):
    """Informe de proyección de cosecha: filas por fundo/especie/variedad/planta y las
    semanas como columnas, con totales y un gráfico de kilos por semana. Por defecto, las
    próximas 3 semanas en kilos de Precosecha: desde hoy hasta hoy + 20 días."""
    try:
        semanas = int(semanas or RESUMEN_SEMANAS)
        if semanas < 1 or semanas > 12:
            return "¿Cuántas semanas necesitas? Puedo armarlo de 1 a 12."
        if fecha_inicio:
            try:
                inicio = _lunes_de(datetime.strptime(fecha_inicio, "%Y-%m-%d").date())
            except (ValueError, TypeError):
                return "No entendí la fecha de inicio. Indícala como DD-MM-YYYY."
        else:
            inicio = date.today()  # «las próximas N semanas» = desde hoy hasta hoy + N*7 - 1 días
        fin = inicio + timedelta(days=semanas * 7 - 1)

        fuentes = [f for f in (fuentes or ["precosecha"]) if f in FUENTES_PROYECCION] or ["precosecha"]
        dimensiones = [d for d in (agrupar_por or DIMENSIONES_PROYECCION_POR_DEFECTO)
                       if d in DIMENSIONES_SQL] or DIMENSIONES_PROYECCION_POR_DEFECTO

        filtros = {"Especie": especie, "Variedad": variedad, "Productor": productor,
                   "Packing": packing, "Grupo": grupo}
        productores_zonal, zonal_real = None, None
        if zonal:
            _sql, productores_zonal, zonal_real, error_zonal = _filtro_zonal(zonal)
            if error_zonal:
                return error_zonal
        filas = obtener_proyeccion(inicio, fin, dimensiones, fuentes, filtros, alcance,
                                   productores=productores_zonal)
        if not filas:
            return (f"No hay datos de {_enumerar(FUENTES_PROYECCION[f][1] for f in fuentes)} "
                    f"entre el {inicio.strftime('%d-%m-%Y')} y el "
                    f"{fin.strftime('%d-%m-%Y')} con esos filtros.")

        encabezados, filas_tabla = armar_tabla_proyeccion(filas, dimensiones, fuentes)
        filtros_desc = [f"{etiqueta} {valor}" for etiqueta, valor in (
            ("especie", traducir_especie(especie) if especie else None), ("variedad", variedad),
            ("productor", productor), ("planta", packing), ("grupo", grupo), ("zonal", zonal_real),
        ) if valor]
        informe = informe_proyeccion(
            filas, dimensiones, fuentes, encabezados, filas_tabla, inicio, fin,
            filtros_desc=filtros_desc, grafico=grafico, resumen=resumen,
        )
        return enviar_informe(numero_destino, formato, informe)
    except Exception as e:
        logger.exception(f"Error generando informe de proyección: {str(e)}")
        return "Tuve un problema armando el informe. ¿Lo intentamos de nuevo?"

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
            f"SELECT TOP 1 1 FROM {RECEPCION_SQL} AS Recepcion_Consolidada WHERE {' AND '.join(condiciones)}",
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
        cursor.execute(f"SELECT {funcion_sql}(CAST(Fecha AS DATE)) FROM {RECEPCION_SQL} AS Recepcion_Consolidada WHERE {where}", params)
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
# CONSULTAS LIBRES: SQL de solo lectura sobre "datos"
# ============================================================================
#
# Para que el bot responda como un chat de IA (cualquier cruce, ranking o cálculo, no solo
# las consultas programadas), Claude puede escribir un SELECT. Nunca toca la base directo:
# solo puede leer "datos", que es Recepcion_Consolidada + la columna Zonal, YA filtrada por
# los fundos que puede ver quien pregunta. Se valida que sea un único SELECT, sin comentarios
# ni palabras de escritura, y que todo FROM/JOIN apunte a "datos" o a un WITH propio.

MAX_FILAS_SQL = int(os.getenv("MAX_FILAS_SQL", "400"))
TIEMPO_MAX_SQL = int(os.getenv("TIEMPO_MAX_SQL", "60"))
_TOKEN_SQL = re.compile(
    r"\[[^\]]*\]|N?'(?:[^']|'')*'|\"[^\"]*\"|[A-Za-z_@#][\w@#$]*|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"
    r"|<>|!=|<=|>=|\S"
)
_SQL_PROHIBIDAS = {
    "insert", "update", "delete", "merge", "drop", "alter", "create", "truncate", "exec",
    "execute", "grant", "revoke", "deny", "into", "openrowset", "openquery", "opendatasource",
    "openxml", "bulk", "shutdown", "kill", "backup", "restore", "dbcc", "waitfor", "reconfigure",
    "use", "declare", "set", "go", "print", "raiserror", "throw", "begin", "commit", "rollback",
    "transaction", "readtext", "writetext", "updatetext", "dbo", "softland", "information_schema",
    "master", "msdb", "tempdb", "sys",
}
# Palabras que cierran una cláusula FROM (después de ellas una coma ya no introduce un origen).
_SQL_FIN_FROM = {"where", "group", "order", "having", "union", "except", "intersect", "for",
                 "option", "window", "select"}
_SQL_FUNCIONES_TABLA = {"string_split", "openjson", "generate_series"}

def _revisar_origenes(tokens, ctes, bases=("datos", "packing")):
    """Error si algún origen de datos es distinto de "datos" o de un WITH propio. Un origen
    es lo que va después de FROM, JOIN, APPLY o de una coma dentro de la cláusula FROM, en
    cualquier nivel de paréntesis. Se recorre llevando, por nivel, si se está dentro de un
    FROM: así "FROM datos a JOIN datos b ON ..., Recepcion_Consolidada f" también se
    revisa (una versión anterior dejaba de buscar comas al llegar al JOIN, y por ahí se
    podía leer la tabla real sin el filtro de alcance). `bases` son las tablas que este usuario
    tiene habilitadas: la otra se rechaza aquí, porque "packing" también es una columna de
    `datos` y no se puede vetar por la sola palabra."""
    tablas = _texto_tablas(bases)
    dentro_de_from = {0: False}
    profundidad = 0
    esperando_origen = False
    i, n = 0, len(tokens)
    while i < n:
        token = tokens[i]
        palabra = token.lower()
        if esperando_origen:
            esperando_origen = False
            if token != "(":
                nombre = token.strip("[]\"").lower()
                if i + 1 < n and tokens[i + 1] == ".":
                    return f"No puedes leer '{token}.{tokens[i + 2] if i + 2 < n else ''}': solo {tablas}."
                es_funcion = nombre in _SQL_FUNCIONES_TABLA and i + 1 < n and tokens[i + 1] == "("
                if not es_funcion and nombre not in ctes:
                    if nombre in BASES_LECTURA["ambas"]:
                        return (f"La tabla '{nombre}' no está habilitada para este usuario: "
                                f"solo puede consultar {tablas}.")
                    return f"Solo puedes consultar {tablas} (o tus propios WITH); '{token}' no está permitido."
                i += 1
                continue
        if token == "(":
            profundidad += 1
            dentro_de_from[profundidad] = False
        elif token == ")":
            dentro_de_from[profundidad] = False
            profundidad = max(profundidad - 1, 0)
        elif palabra == "from":
            dentro_de_from[profundidad] = True
            esperando_origen = True
        elif palabra in ("join", "apply"):
            esperando_origen = True
        elif token == "," and dentro_de_from.get(profundidad):
            esperando_origen = True
        elif palabra in _SQL_FIN_FROM:
            dentro_de_from[profundidad] = False
        i += 1
    if esperando_origen:
        return "La consulta termina sin indicar de dónde leer."
    return None

# Embarques (vistas Reporte_Embarque_PBI y Embarque-Liquidacion-PBI): información comercial
# (clientes, naves, valores FOB) que Agua Santa decidió mostrar solo a estos perfiles. No
# depende de acceso_total: un zonal con acceso a todos los fundos igual no la ve.
ROLES_EMBARQUES = ("admin", "eas", "gerencia")
TABLAS_EMBARQUES = ("embarques", "embarques_detalle", "embarques_fob")
# Qué tablas de embarques ve cada nivel. El calibre (serie), el productor y la planta solo vienen
# en la vista de liquidación, junto con el FOB: «solo embarques» la ve como `embarques_detalle`,
# sin los valores (FOB, FOB pool ni estado de liquidación). Antes ese nivel perdía la vista
# entera, y con ella el calibre, que no es un dato de plata.
TABLAS_POR_NIVEL = {0: (), 1: ("embarques", "embarques_detalle"), 2: ("embarques", "embarques_fob")}
NOMBRES_RESERVADOS_SQL = {"datos", "packing", "conversiones", *TABLAS_EMBARQUES}

NIVELES_EMBARQUES = {"no": 0, "sin_fob": 1, "fob": 2}
NOMBRE_NIVEL_EMBARQUES = {nivel: nombre for nombre, nivel in NIVELES_EMBARQUES.items()}

def nivel_embarques(numero):
    """Qué puede ver de embarques: 0 = nada; 1 = la tabla `embarques` (cajas, naves,
    destinos), sin valores FOB; 2 = también `embarques_fob`. El admin ve todo; EAS y gerencia,
    lo que se les haya elegido (por defecto todo); los demás perfiles, nada. Sin número es una
    prueba local de administración (ver /test/mensaje)."""
    if numero is None:
        return 2
    rol = rol_de(numero)
    if rol not in ROLES_EMBARQUES:
        return 0
    if rol == "admin":
        return 2
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute("SELECT ver_embarques FROM numeros_permitidos WHERE numero = ?",
                            (normalizar_numero(numero),)).fetchone()
        conn.close()
    except Exception as e:
        logger.error(f"Error leyendo el nivel de embarques: {str(e)}")
        return 0
    return 2 if not fila or fila[0] is None else max(0, min(2, int(fila[0])))

def _tablas_embarques(embarques):
    """Tablas de embarques permitidas para un nivel (True = todas, como antes)."""
    nivel = 2 if embarques is True else int(embarques or 0)
    return TABLAS_POR_NIVEL[max(0, min(2, nivel))]

# Bases que lee cada usuario, de cualquier perfil. 'cosecha' = la tabla `datos` y las herramientas
# de cosecha; 'packing' = la tabla `packing`. Los envíos automáticos (cuestionarios, resúmenes y
# avisos) no dependen de esto: siguen según el perfil.
BASES_LECTURA = {"ambas": ("datos", "packing"), "cosecha": ("datos",), "packing": ("packing",)}
NOMBRE_BASES = {tablas: nombre for nombre, tablas in BASES_LECTURA.items()}
BASES_POR_DEFECTO = "cosecha"

def bases_de(numero):
    """Tablas que puede leer: el admin, ambas; los demás, lo que se les haya elegido. Sin elegir
    = solo cosecha, así nadie ve Packing hasta que se le habilite. Sin número es una prueba
    local de administración (ver /test/mensaje)."""
    if numero is None:
        return BASES_LECTURA["ambas"]
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute("SELECT rol, ver_bases FROM numeros_permitidos WHERE numero = ?",
                            (normalizar_numero(numero),)).fetchone()
        conn.close()
    except Exception as e:
        logger.error(f"Error leyendo las bases del usuario: {str(e)}")
        return BASES_LECTURA[BASES_POR_DEFECTO]
    if fila and fila[0] == "admin":
        return BASES_LECTURA["ambas"]
    return BASES_LECTURA.get(fila[1] if fila else None, BASES_LECTURA[BASES_POR_DEFECTO])

def _texto_tablas(tablas):
    """"la tabla 'datos'" o "las tablas 'datos' y 'packing'", para los mensajes de error."""
    tablas = list(tablas)
    if len(tablas) == 1:
        return f"la tabla '{tablas[0]}'"
    return "las tablas " + ", ".join(f"'{t}'" for t in tablas[:-1]) + f" y '{tablas[-1]}'"

# El historial de confirmaciones y ajustes lo consultan los mismos perfiles que ven todo.
ROLES_CONFIRMACIONES = ("admin", "eas", "gerencia")

def puede_ver_confirmaciones(numero):
    """Sin número es una prueba local de administración (ver /test/mensaje)."""
    return numero is None or rol_de(numero) in ROLES_CONFIRMACIONES

def validar_consulta_sql(sql, tablas_extra=(), bases=BASES_LECTURA["ambas"]):
    """Devuelve (consulta, None) si es un SELECT de solo lectura sobre las bases de este usuario
    (y las tablas_extra que tenga permitidas), o (None, motivo) si no."""
    texto = (sql or "").strip()
    while texto.endswith(";"):
        texto = texto[:-1].rstrip()
    if not texto:
        return None, "La consulta está vacía."
    if len(texto) > 12000:
        return None, "La consulta es demasiado larga."
    sin_literales = re.sub(r"N?'(?:[^']|'')*'", "''", texto)
    if "--" in sin_literales or "/*" in sin_literales:
        return None, "No uses comentarios en la consulta."
    tokens = _TOKEN_SQL.findall(texto)
    if ";" in tokens:
        return None, "Envía una sola consulta (sin ';')."
    if not tokens or tokens[0].upper() not in ("SELECT", "WITH"):
        return None, "Solo se permiten consultas SELECT (o WITH ... SELECT)."
    for token in tokens:
        if token.startswith(("'", "N'")):
            continue
        palabra = token.strip("[]\"").lower()
        if (palabra in _SQL_PROHIBIDAS or palabra.startswith(("sp_", "xp_", "fn_", "@", "#"))
                or (palabra.startswith("sys") and not palabra.startswith("sysdatetime"))):
            return None, f"La consulta usa '{token}', que no está permitido (solo lectura)."
    ctes = {re.sub(r"[\[\]\"]", "", m).lower() for m in re.findall(
        r"(?:\bwith|,)\s*(\[[^\]]+\]|[A-Za-z_]\w*)\s*(?:\([^()]*\))?\s+as\s*\(", sin_literales, re.I)}
    reservados = ctes & NOMBRES_RESERVADOS_SQL
    if reservados:
        return None, f"No definas un WITH llamado '{sorted(reservados)[0]}': ese nombre ya es una tabla."
    permitidas = {*bases, *tablas_extra, "conversiones"}
    vedadas = {t.strip("[]\"").lower() for t in tokens} & (set(TABLAS_EMBARQUES) - permitidas)
    if vedadas:
        if "embarques" in permitidas:
            return None, ("Los valores FOB (tabla embarques_fob) no están habilitados para este usuario: para "
                          "productor, planta o calibre usa `embarques_detalle`, que trae lo mismo sin los valores.")
        return None, ("Las tablas de embarques (embarques, embarques_fob) no están habilitadas para "
                      "este usuario.")
    error = _revisar_origenes(tokens, ctes | permitidas, bases)
    if error:
        return None, error
    return texto, None

def _sql_expr_temporada(columna_fecha="rc.Fecha", columna_temporada="rc.Temporada"):
    """Temporada oficial según la fecha (semana ISO 44 a 43, la misma regla de
    rango_temporada que usan todas las consultas del bot). La columna Temporada de la base
    no siempre calza: por ejemplo, ya asigna a la temporada siguiente la cereza temprana
    recibida a mediados de octubre."""
    vigente = TEMPORADA_ACTUAL or temporada_de(date.today())
    casos = []
    for temporada in range(2018, vigente + 3):
        inicio, fin = rango_temporada(temporada)
        # 'AAAAMMDD' y no 'AAAA-MM-DD': comparado con un datetime, SQL Server en español lee
        # '2025-10-27' como año-día-mes y falla por fecha fuera de rango.
        casos.append(f"WHEN {columna_fecha} >= '{inicio.strftime('%Y%m%d')}' AND "
                     f"{columna_fecha} < '{(fin + timedelta(days=1)).strftime('%Y%m%d')}' THEN {temporada}")
    return f"(CASE {' '.join(casos)} ELSE {columna_temporada} END)"

def _sql_via(columna):
    return (f"CASE {columna} WHEN 'Sea' THEN N'Marítima' WHEN 'Air' THEN N'Aérea' "
            f"WHEN 'Earth' THEN N'Terrestre' ELSE {columna} END")

def _ctes_embarques():
    """Las dos vistas de embarques con nombres de columna en castellano. Tienen las mismas
    filas (una por pallet o parte de pallet embarcado): la segunda agrega productor, planta y
    valores FOB, pero no trae contenedor ni pallets."""
    embarques = (
        "embarques AS (SELECT CAST(e.Temporada AS INT) AS Temporada, e.[Commodity Name] AS Especie, "
        "e.[Variety Name] AS Variedad, e.Variedad_tipo AS TipoVariedad, e.Cases AS Cajas, "
        "e.Cases_Eq AS CajasEq, e.Pallets, e.[Pallet Total] AS CajasPorPallet, "
        "NULLIF(e.Container, '') AS Contenedor, e.[Receiver name] AS Recibidor, e.[Vessel name] AS Nave, "
        f"{_sql_via('e.[Vessel type]')} AS Via, e.[Departure date] AS FechaZarpe, "
        "CAST(e.WeekOnly AS INT) AS SemanaZarpe, e.[Arrival port] AS Puerto, e.ETA, "
        "e.Market AS Mercado, NULLIF(e.SaleMarket, '') AS PaisVenta "
        "FROM [Reporte_Embarque_PBI] e)"
    )
    def liquidacion(nombre, valores):
        return (
            f"{nombre} AS (SELECT CAST(l.Temporada AS INT) AS Temporada, l.[Grower Name] AS Productor, "
            f"l.growerFamily__name AS Grupo, {_sql_expr_zonal('l.[Grower Name]')} AS Zonal, "
            "l.[PackingSite Name] AS Planta, l.[Commodity Name] AS Especie, l.Variedad_Agronomica AS Variedad, "
            "l.Variedad_tipo AS TipoVariedad, l.EqSerie AS Serie, l.Cases AS Cajas, l.Cases_Eq AS CajasEq, "
            + valores +
            "l.Arribado, l.[Receiver name] AS Recibidor, l.[Receiver code] AS CodigoRecibidor, l.[Vessel name] AS Nave, "
            f"{_sql_via('l.[Vessel type]')} AS Via, l.[Departure date] AS FechaZarpe, "
            "CAST(l.WeekOnly AS INT) AS SemanaZarpe, l.[Arrival port] AS Puerto, "
            "l.[Arrival country] AS PaisLlegada, l.ETA, l.[Arrival date] AS FechaLlegada, l.Market AS Mercado "
            "FROM [Embarque-Liquidacion-PBI] l)"
        )
    return {
        "embarques": embarques,
        # La misma vista de liquidación, sin los valores: para quien ve embarques pero no el FOB.
        "embarques_detalle": liquidacion("embarques_detalle", ""),
        "embarques_fob": liquidacion("embarques_fob", "l.[FOB (usd)] AS FOB, l.[Pool FOB (usd)] AS FOBPool, "
                                                      "l.Estatus AS Estado, "),
    }

# Packing_Consolidado: el embalaje de la fruta (la contraparte de la recepción). Se deja
# fuera 'Packing Edge' por decisión de Edson (05-11-2026); el resto de las fuentes sí se
# consulta. No tiene el problema de filas duplicadas sin kilos de Recepcion_Consolidada
# (verificado: 1 fila de 91.377 sin KgsEmbalados), así que no se filtra nada más.
BASE_ORIGEN_PACKING_EXCLUIDA = "Packing Edge"

def _cte_packing(alcance):
    """Tabla `packing` con nombres en castellano y acotada al alcance del usuario, igual que
    `datos`. Las columnas de texto vienen de campos nchar con espacios de relleno, por eso el
    LTRIM/RTRIM: sin él, Categoria llega como 'CAT 1                 '."""
    condiciones = [f"pc.[Base origen] <> N'{BASE_ORIGEN_PACKING_EXCLUIDA}'"]
    if isinstance(alcance, AlcancePlanta):
        condiciones.append(_sql_plantas(alcance, columna="pc.Packing_name", literales=True)[0])
    elif alcance is not None:
        sql_fundos, _ = _sql_fundos(alcance, columna="pc.Productor", literales=True) if alcance else ("1 = 0", [])
        condiciones.append(sql_fundos)
    temporada = _sql_expr_temporada("pc.Fecha", "pc.Temporada")
    return ("packing AS (SELECT "
            f"{temporada} AS Temporada, pc.Temporada AS TemporadaBase, "
            "LTRIM(RTRIM(pc.[Base origen])) AS [Base Origen], pc.Especie, pc.Variedad, "
            "pc.Variedad_Agronomica AS [Variedad Agronomica], pc.Productor, "
            "pc.Familia_Productor AS Grupo, "
            f"{_sql_expr_zonal('pc.Productor')} AS Zonal, "
            "LTRIM(RTRIM(pc.Packing_name)) AS Packing, pc.Fecha, pc.Semana, "
            "pc.Bultos, pc.Cajas_Eq AS CajasEq, pc.KgsEmbalados, pc.KgsVaciados, "
            "NULLIF(LTRIM(RTRIM(pc.Categoria)), '') AS Categoria, "
            "NULLIF(LTRIM(RTRIM(pc.Calibre)), '') AS Calibre "
            f"FROM [Packing_Consolidado] pc WHERE {' AND '.join(condiciones)})")

def _consulta_con_datos(consulta, alcance, embarques=False, bases=BASES_LECTURA["ambas"]):
    """Antepone el WITH "datos": la tabla real + Zonal, con la temporada oficial por fecha,
    sin las filas duplicadas sin kilos (las mismas que excluyen todas las consultas del bot)
    y acotada al alcance del usuario. Solo se definen las bases de este usuario: la otra no
    existe para su consulta. Con embarques=True agrega también las tablas de embarques (solo
    para los perfiles de ROLES_EMBARQUES)."""
    condiciones = ["rc.KgsRecepcionados IS NOT NULL"]
    if isinstance(alcance, AlcancePlanta):
        condiciones.append(_sql_plantas(alcance, columna="rc.Packing", literales=True)[0])
    elif alcance is not None:
        sql_fundos, _ = _sql_fundos(alcance, columna="rc.Productor", literales=True) if alcance else ("1 = 0", [])
        condiciones.append(sql_fundos)
    ctes = []
    if "datos" in bases:
        ctes.append(f"datos AS (SELECT {_sql_expr_temporada()} AS Temporada, rc.Temporada AS TemporadaBase, "
                    f"rc.[Base Origen], rc.Especie, rc.Variedad, rc.[Variedad Agronomica], rc.Productor, "
                    f"rc.Grupo, {_sql_expr_zonal('rc.Productor')} AS Zonal, rc.Packing, rc.Fecha, rc.Semana, "
                    f"rc.Envase, rc.Bultos, rc.KgsRecepcionados, rc.Terminado, rc.tipo_despacho, rc.CajasEq, rc.Trisemanal "
                    f"FROM {RECEPCION_SQL} rc WHERE {' AND '.join(condiciones)})")
    if "packing" in bases:
        ctes.append(_cte_packing(alcance))
    ctes.append(_cte_conversiones())
    if embarques:
        ctes += [cte for nombre, cte in _ctes_embarques().items() if nombre in _tablas_embarques(embarques)]
    resto = re.sub(r"^\s*with\s+", "", consulta, flags=re.I)
    if resto != consulta:
        return f"WITH {', '.join(ctes)}, {resto}"
    return f"WITH {', '.join(ctes)} {consulta}"

def _mensaje_error_sql(error):
    """Deja solo el mensaje útil de SQL Server (sin los prefijos [Microsoft][ODBC...])."""
    texto = str(error.args[1] if len(getattr(error, "args", ())) > 1 else error)
    texto = re.sub(r"\[[^\]]*\]", "", texto)
    texto = re.sub(r"\(\d+\)\s*\(SQL\w+\)", "", texto)
    return " ".join(texto.split())[:400]

def ejecutar_consulta_sql(sql, alcance, max_filas=MAX_FILAS_SQL, embarques=False, bases=BASES_LECTURA["ambas"]):
    """({"columnas", "filas", "truncado"}, None) o (None, error). embarques=True permite
    además las tablas de embarques (quien llama ya verificó el perfil); bases son las tablas
    de cosecha y packing que este usuario tiene habilitadas."""
    consulta, error = validar_consulta_sql(sql, _tablas_embarques(embarques), bases=bases)
    if error:
        return None, error
    conn = conectar_sql()
    if not conn:
        return None, "Error de conexión a base de datos."
    try:
        conn.timeout = TIEMPO_MAX_SQL
        cursor = conn.cursor()
        cursor.execute(_consulta_con_datos(consulta, alcance, embarques=embarques, bases=bases))
        if not cursor.description:
            return None, "La consulta no devolvió una tabla."
        columnas = [d[0] or f"columna_{i + 1}" for i, d in enumerate(cursor.description)]
        filas = cursor.fetchmany(max_filas + 1)
        truncado = len(filas) > max_filas
        return {"columnas": columnas, "filas": [list(f) for f in filas[:max_filas]],
                "truncado": truncado}, None
    except pyodbc.Error as e:
        return None, f"SQL Server rechazó la consulta: {_mensaje_error_sql(e)}"
    except Exception as e:
        logger.exception(f"Error ejecutando consulta libre: {str(e)}")
        return None, f"No pude ejecutar la consulta: {str(e)[:300]}"
    finally:
        conn.close()

def _texto_crudo(valor):
    """Valor tal cual para Claude (punto decimal, sin separador de miles), para que no
    confunda 12.345 (miles) con 12,345."""
    valor = _valor_normalizado(valor)
    if isinstance(valor, float):
        return f"{valor:.2f}".rstrip("0").rstrip(".")
    if hasattr(valor, "isoformat"):
        return valor.isoformat()
    return str(valor)

def consultar_sql(sql, alcance, embarques=False, bases=BASES_LECTURA["ambas"]):
    """Resultado de una consulta libre como texto para Claude."""
    resultado, error = ejecutar_consulta_sql(sql, alcance, embarques=embarques, bases=bases)
    if error:
        return f"ERROR: {error}"
    if not resultado["filas"]:
        return "La consulta no devolvió filas (sin datos para esos filtros y periodo)."
    lineas = [" | ".join(resultado["columnas"])]
    lineas += [" | ".join(_texto_crudo(v) for v in fila) for fila in resultado["filas"]]
    nota = ""
    if resultado["truncado"]:
        nota = (f"\n(Se muestran solo las primeras {MAX_FILAS_SQL} filas: agrupa más o usa TOP "
                f"si necesitas el total.)")
    return f"{len(resultado['filas'])} fila(s):\n" + "\n".join(lineas) + nota

# Historial de confirmaciones: vive en la base local del bot (SQLite), no en SQL Server. Se
# lee con una conexión de solo lectura que además solo deja leer la tabla `confirmaciones`.
TIEMPO_MAX_CONFIRMACIONES = 10

def ejecutar_consulta_confirmaciones(sql, max_filas=MAX_FILAS_SQL):
    """({"columnas", "filas", "truncado"}, None) o (None, error)."""
    consulta = (sql or "").strip().rstrip(";").strip()
    if not consulta:
        return None, "La consulta está vacía."
    if ";" in consulta:
        return None, "Va una sola consulta, sin ';'."
    if not re.match(r"(?is)^(select|with)\b", consulta):
        return None, "Solo se permiten consultas SELECT (o WITH ... SELECT) sobre `confirmaciones`."
    recursivo = getattr(sqlite3, "SQLITE_RECURSIVE", 33)
    try:
        conn = sqlite3.connect(Path(DB_LOCAL_PATH).as_uri() + "?mode=ro", uri=True, timeout=10)
    except sqlite3.Error as e:
        return None, f"No pude abrir el historial: {e}"
    # Las tablas reales de la base: de ellas solo se puede leer `confirmaciones`. Lo que no es
    # una tabla real son los nombres que define la propia consulta (WITH ult AS ...).
    reales = {f[0].lower() for f in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")}

    def autorizar(accion, arg1, _arg2, _base, _origen):
        if accion in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION, recursivo):
            return sqlite3.SQLITE_OK
        if accion == sqlite3.SQLITE_READ:
            tabla = (arg1 or "").lower()
            if tabla == "confirmaciones" or (tabla not in reales and not tabla.startswith("sqlite_")):
                return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    try:
        inicio = time.monotonic()
        conn.set_progress_handler(lambda: 1 if time.monotonic() - inicio > TIEMPO_MAX_CONFIRMACIONES else 0, 20000)
        conn.set_authorizer(autorizar)
        cursor = conn.execute(consulta)
        columnas = [d[0] for d in cursor.description or []]
        filas = cursor.fetchmany(max_filas + 1)
        return {"columnas": columnas, "filas": [list(f) for f in filas[:max_filas]],
                "truncado": len(filas) > max_filas}, None
    except sqlite3.DatabaseError as e:
        texto = str(e)
        if "not authorized" in texto:
            texto = "solo se puede leer la tabla `confirmaciones`."
        elif "interrupted" in texto:
            texto = f"la consulta tardó más de {TIEMPO_MAX_CONFIRMACIONES} s; acótala."
        return None, f"Error en la consulta: {texto}"
    finally:
        conn.close()

EVENTO_HISTORIAL = {"confirmado": "confirmó", "ajustado": "ajuste", "corregido": "corrección",
                    "anulado": "anuló su ajuste", "reporte": "aviso sin cuestionario"}

def historial_confirmaciones(numero_consultante, persona=None, productor=None, desde=None, hasta=None,
                             incluir_confirmaciones=True, max_lineas=60):
    """Una línea por confirmación, ajuste, corrección o anulación, en orden. Admin, EAS y gerencia
    ven todo; un zonal o productor, lo que reportó él y lo que toca sus fundos."""
    try:
        hoy = date.today()
        hasta_d = datetime.strptime(hasta, "%Y-%m-%d").date() if hasta else hoy
        desde_d = datetime.strptime(desde, "%Y-%m-%d").date() if desde else hasta_d - timedelta(days=6)
    except ValueError:
        return "No entendí las fechas: usa AAAA-MM-DD."
    eventos = list(EVENTO_HISTORIAL) if incluir_confirmaciones else [e for e in EVENTO_HISTORIAL if e != "confirmado"]
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.row_factory = sqlite3.Row
    filas = conn.execute(
        f"SELECT fecha_hora, fecha, evento, turno, numero, nombre, productores, aviso, detalle, texto_usuario "
        f"FROM confirmaciones WHERE evento IN ({', '.join('?' * len(eventos))}) "
        f"AND date(fecha_hora) BETWEEN ? AND ? ORDER BY id",
        (*eventos, desde_d.isoformat(), hasta_d.isoformat())
    ).fetchall()
    conn.close()
    # Alcance: el equipo ve todo; un zonal o productor, lo propio y lo de sus fundos.
    if numero_consultante and rol_de(numero_consultante) not in ROLES_CONFIRMACIONES \
            and not tiene_acceso_total(numero_consultante):
        propio = normalizar_numero(numero_consultante)
        fundos = fundos_efectivos(propio)
        filas = [f for f in filas if f["numero"] == propio or any(
            _fundo_coincide(x, p) for x in fundos for p in (f["productores"] or "").split("; ") if p)]
    if persona:
        buscado = _normalizar_nombre(persona)
        filas = [f for f in filas if buscado in _normalizar_nombre(f["nombre"] or "")]
    if productor:
        buscado = _normalizar_nombre(productor)
        filas = [f for f in filas if buscado in _normalizar_nombre(f["productores"] or "")
                 or buscado in _normalizar_nombre(f["aviso"] or f["detalle"] or "")]
    if not filas:
        return f"No hay confirmaciones ni ajustes registrados del {desde_d:%d-%m} al {hasta_d:%d-%m} con esos filtros."
    recortadas = len(filas) > max_lineas
    filas = filas[-max_lineas:]
    titulo = f"Historial del {desde_d:%d-%m} al {hasta_d:%d-%m}" + (f" ({persona})" if persona else "")
    lineas = [f"{titulo}: {len(filas)} hecho(s)" + (f", los últimos {max_lineas}" if recortadas else "")]
    for f in filas:
        cuando = datetime.strptime(f["fecha_hora"][:16], "%Y-%m-%d %H:%M").strftime("%d-%m %H:%M")
        que = EVENTO_HISTORIAL.get(f["evento"], f["evento"])
        origen = TURNO_AVISO.get(f["turno"] or "", "")
        texto = _aviso_corto(f["aviso"] or f["detalle"] or f["texto_usuario"] or "", 160)
        lineas.append(f"• {cuando} · {f['nombre'] or f['numero']} · {que}"
                      + (f" ({origen})" if origen and f["evento"] != "reporte" else "")
                      + (f": {texto}" if texto and f["evento"] != "confirmado" else ""))
    return "\n".join(lineas)

def consultar_confirmaciones(sql):
    """Resultado de una consulta al historial como texto para Claude."""
    resultado, error = ejecutar_consulta_confirmaciones(sql)
    if error:
        return f"ERROR: {error}"
    if not resultado["filas"]:
        return "La consulta no devolvió filas."
    lineas = [" | ".join(resultado["columnas"])]
    lineas += [" | ".join(_texto_crudo(v) for v in fila) for fila in resultado["filas"]]
    nota = (f"\n(Se muestran solo las primeras {MAX_FILAS_SQL} filas: agrupa más o usa LIMIT.)"
            if resultado["truncado"] else "")
    return f"{len(resultado['filas'])} fila(s):\n" + "\n".join(lineas) + nota

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

def cargar_variantes_productor():
    """{clave_fundo: [nombres tal cual están en cada base]}. Se leen las dos tablas porque un
    mismo fundo puede estar escrito distinto en cada una, y si no se juntan el filtro por
    fundos deja fuera esas filas sin avisar."""
    try:
        conn = conectar_sql()
        if not conn:
            return {}
        cursor = conn.cursor()
        nombres = set()
        for tabla in ("[Recepcion_Consolidada]", "[Packing_Consolidado]"):
            try:
                cursor.execute(f"SELECT DISTINCT Productor FROM {tabla} WHERE Productor IS NOT NULL")
                nombres.update(f[0].strip() for f in cursor.fetchall() if f[0] and f[0].strip())
            except Exception as e:
                logger.error(f"No pude leer productores de {tabla}: {str(e)}")
        conn.close()
        variantes = {}
        for nombre in nombres:
            variantes.setdefault(_clave_fundo(nombre), set()).add(nombre)
        repetidos = {k: v for k, v in variantes.items() if len(v) > 1}
        if repetidos:
            logger.info(f"Fundos escritos de más de una forma: {len(repetidos)} "
                        f"({', '.join(sorted(repetidos)[:3])}...)")
        return {k: sorted(v) for k, v in variantes.items()}
    except Exception as e:
        logger.error(f"Error cargando variantes de productor: {str(e)}")
        return {}

VARIANTES_PRODUCTOR = cargar_variantes_productor()

def variantes_de_fundo(fundo):
    """Todas las formas en que ese fundo aparece escrito en las bases. Devuelve [] si no se
    reconoce (ahí el filtro cae a la coincidencia parcial de siempre)."""
    return VARIANTES_PRODUCTOR.get(_clave_fundo(fundo), [])
sincronizar_nombres_zonales()

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

def temporada_de(fecha):
    """Temporada a la que pertenece una fecha, según la definición del negocio: la temporada
    N va de la semana ISO 44 del año N-1 a la 43 del año N. Es la inversa de rango_temporada.
    Se usa el año ISO y no el del calendario: a principios de enero la semana ISO todavía
    puede ser del año anterior (ej. 2027-01-01 es la semana 53 de 2026), y mezclarlos
    adelantaba la temporada un año entero."""
    anio_iso, semana_iso, _ = fecha.isocalendar()
    return anio_iso + 1 if semana_iso >= 44 else anio_iso

def obtener_temporada_actual():
    """Temporada vigente hoy. Se calcula con la definición del negocio y NO con el MAX de la
    columna Temporada: la base etiqueta como temporada siguiente filas cuya fecha todavía no
    llega al inicio de esa temporada, y eso dejaba un rango invertido en los días previos
    (verificado el 21-10-2026: devolvía 2027, cuyo rango parte el 26-10, y las consultas de
    'esta temporada' quedaban vacías)."""
    try:
        return temporada_de(date.today())
    except Exception as e:
        logger.error(f"Error obteniendo temporada actual: {str(e)}")
        return None

TEMPORADA_ACTUAL = obtener_temporada_actual()

def recargar_catalogos():
    """Los catálogos se cargan al arrancar: si el bot arrancó con la base desconectada quedaron
    vacíos. Al reconectar se vuelven a leer (si una lectura falla, se conserva la anterior)."""
    global VARIEDADES_CONOCIDAS, PACKINGS_CONOCIDOS, ENVASES_CONOCIDOS, PRODUCTORES_CONOCIDOS, GRUPOS_CONOCIDOS
    global VARIANTES_PRODUCTOR
    VARIEDADES_CONOCIDAS = cargar_variedades_conocidas() or VARIEDADES_CONOCIDAS
    PACKINGS_CONOCIDOS = cargar_packings_conocidos() or PACKINGS_CONOCIDOS
    ENVASES_CONOCIDOS = cargar_envases_conocidos() or ENVASES_CONOCIDOS
    PRODUCTORES_CONOCIDOS = cargar_productores_conocidos() or PRODUCTORES_CONOCIDOS
    GRUPOS_CONOCIDOS = cargar_grupos_conocidos() or GRUPOS_CONOCIDOS
    VARIANTES_PRODUCTOR = cargar_variantes_productor() or VARIANTES_PRODUCTOR
    sincronizar_nombres_zonales()
    return {"variedades": len(VARIEDADES_CONOCIDAS), "productores": len(PRODUCTORES_CONOCIDOS)}

def normalizar_variedad(variedad_usuario):
    """Limpia espacios; la traducción al nombre exacto ya la hace Claude usando VARIEDADES_CONOCIDAS"""
    return variedad_usuario.strip()

# Opciones comunes de los informes en archivo: por defecto llevan un gráfico de totales y
# no llevan comentario; el usuario puede pedir otro gráfico o que se agregue un análisis.
GRAFICO_INFORME_SCHEMA = {
    "type": "object",
    "description": "Solo si el usuario pide un gráfico distinto al de por defecto (o ninguno). Por defecto el informe ya trae un gráfico pequeño con los totales.",
    "properties": {
        "tipo": {"type": "string", "enum": ["barras", "barras_horizontales", "lineas", "torta", "ninguno"],
                 "description": "barras (columnas), barras_horizontales (mejor para nombres largos), lineas (evolución en el tiempo), torta (participación, máx. 6 porciones) o ninguno."},
        "por": {"type": "string", "enum": ["especie", "variedad", "productor", "packing", "grupo", "zonal", "fecha", "semana"],
                "description": "Qué va en el eje del gráfico. Debe ser una de las dimensiones del informe (o 'semana' en la proyección)."},
        "titulo": {"type": "string", "description": "Título del gráfico, si el usuario pidió uno."},
    },
}
RESUMEN_INFORME_SCHEMA = {
    "type": "string",
    "description": "Comentario o análisis para la sección 'Resumen' del informe (2 a 6 frases o viñetas con '- '). Pásalo cuando el usuario pida conclusiones/comentarios, o cuando ya tengas cifras de una consulta previa que valga la pena destacar. Usa SOLO cifras que hayas obtenido de herramientas en esta conversación; nunca inventes ni estimes números.",
}

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
                    "items": {"type": "string", "enum": ["especie", "variedad", "productor", "packing", "grupo", "zonal", "fecha", "semana"]},
                    "description": "Dimensiones exactas por las que agrupar, en el orden pedido por el usuario. Ej. 'resumen por especie' -> ['especie']; 'informe con fecha, estimado y real' -> ['fecha']; 'total por productor y variedad' -> ['productor','variedad']; 'por zonal' -> ['zonal']; 'solo el total, sin desglose' -> [] (arreglo vacío); 'por semana' -> ['semana'] (agrupa por número de semana calendario, la misma columna Semana de la base, en el orden natural de la temporada 44..52, 1..43)."
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
                "zonal": {
                    "type": "string",
                    "description": "Filtro opcional: solo los productores de ese zonal (nombre de la lista de zonales, ej. 'Joaquin Llano')."
                },
                "formato_archivo": {
                    "type": "string",
                    "enum": ["excel", "pdf"],
                    "description": "Pasar SOLO si el usuario pide el resultado como archivo ('mándamelo en Excel', 'en PDF', 'un informe', 'un archivo con el detalle completo'). Se envía un INFORME con cifras clave, un gráfico de totales y la tabla completa (sin el tope del chat). Si no pide archivo, omitir."
                },
                "grafico": GRAFICO_INFORME_SCHEMA,
                "resumen": RESUMEN_INFORME_SCHEMA,
                "kg_por_caja_eq": {
                    "type": "number",
                    "description": "Cuántos kilos equivalen a una Caja Eq para la variedad consultada. Pasarlo SOLO cuando el usuario ya respondió esa cifra, después de que la herramienta la haya pedido. No inventarlo ni asumir un valor típico."
                }
            },
            "required": ["agrupar_por"]
        }
    },
]

TOOL_PROYECCION = {
    "name": "generar_informe_proyeccion",
    "description": "Genera un INFORME DE PROYECCIÓN DE COSECHA como archivo PDF o Excel: cifras clave, un gráfico pequeño con los kilos de cada semana y la tabla con fundo/especie/variedad/planta como filas y las SEMANAS como columnas, con totales. Usar cuando el usuario pida una proyección, un informe o un PDF de lo que viene ('mándame la proyección de cosecha', 'el informe de las próximas semanas en PDF', 'proyección por variedad', 'proyección de la zona de Joaquín'). Por defecto son las próximas 3 semanas en kilos de Precosecha. ANTES de llamarla, si el usuario no fue específico, PREGÚNTALE cómo lo quiere: cuántas semanas, qué fuentes (precosecha, trisemanal, real) y qué columnas quiere como filas; ofrécele el formato por defecto para que solo tenga que confirmar.",
    "input_schema": {
        "type": "object",
        "properties": {
            "formato": {
                "type": "string",
                "enum": ["pdf", "excel"],
                "description": "Formato del archivo. 'pdf' por defecto; 'excel' si el usuario lo pide para trabajar los datos."
            },
            "semanas": {
                "type": "integer",
                "description": "Cuántas semanas hacia adelante cubrir (1 a 12). Por defecto 3."
            },
            "fecha_inicio": {
                "type": "string",
                "description": "YYYY-MM-DD. Solo si el usuario pide un periodo distinto al que viene; se usa el lunes de esa semana. Si no, omitir y parte de esta semana."
            },
            "fuentes": {
                "type": "array",
                "items": {"type": "string", "enum": ["precosecha", "trisemanal", "real"]},
                "description": "Qué columnas de cifras incluir en cada semana. Por defecto ['precosecha']. Agregar 'trisemanal' y/o 'real' solo si el usuario los pide."
            },
            "agrupar_por": {
                "type": "array",
                "items": {"type": "string", "enum": ["productor", "especie", "variedad", "packing", "grupo", "zonal"]},
                "description": "Qué columnas forman las filas, en orden. Por defecto ['productor','especie','variedad','packing'] (fundo, especie, variedad, planta). Usar menos si el usuario quiere algo más resumido; ['zonal'] o ['zonal','productor'] para verlo por zonal."
            },
            "especie": {"type": "string", "description": "Filtro opcional de especie, nombre EXACTO de la lista conocida."},
            "variedad": {"type": "string", "description": "Filtro opcional de variedad, nombre EXACTO de la lista conocida."},
            "productor": {"type": "string", "description": "Filtro opcional de productor/fundo, nombre EXACTO de la lista conocida."},
            "packing": {"type": "string", "description": "Filtro opcional de planta/packing, nombre EXACTO de la lista conocida."},
            "grupo": {"type": "string", "description": "Filtro opcional de grupo, nombre EXACTO de la lista conocida."},
            "zonal": {"type": "string", "description": "Filtro opcional: solo los productores de ese zonal (nombre de la lista de zonales)."},
            "grafico": GRAFICO_INFORME_SCHEMA,
            "resumen": RESUMEN_INFORME_SCHEMA,
        }
    },
}

# La descripción de consultar_sql se arma con lo que el usuario puede leer (ver _herramienta_sql):
# una tabla que no tiene habilitada ni se describe.
_SQL_INTRO = (
        "Consulta LIBRE de solo lectura (T-SQL de SQL Server) para todo lo que las otras herramientas no cubren: "
        "rankings, cruces, comparaciones entre temporadas, avance %, promedios, kg por bin, qué variedad está más "
        "atrasada, top N, etc. "
)
_SQL_DATOS = (
        "Escribe UN solo SELECT (o WITH ... SELECT) sobre la tabla `datos`, que ya viene "
        "filtrada por lo que este usuario puede ver. Columnas de `datos`: Temporada (int, la temporada oficial "
        "según la fecha: semana ISO 44 a 43, la misma que usan las demás herramientas; úsala para 'esta "
        "temporada', 'la pasada', comparar temporadas), TemporadaBase (la que trae la base, que ya asigna a la "
        "temporada siguiente la cereza temprana de octubre; úsala solo si la piden), [Base Origen] "
        "('Recepción Planta' = real, 'Estim Primavera' = Precosecha, 'Trisemanal' = estimación de cosecha por día, "
        "'Estim Invierno'), Especie (en "
        "inglés: CHERRY, GRAPE...), Variedad, [Variedad Agronomica], Productor (= fundo), Grupo, Zonal (calculada: "
        "zonal responsable del productor, 'Sin zonal' si no tiene), Packing (= planta), Fecha (datetime), Semana "
        "(float, semana ISO), Envase (nchar: BINS, TOTES, CAJA EQ...), Bultos (float, unidades de envase), "
        "KgsRecepcionados (float, kilos), Terminado, tipo_despacho, CajasEq (float, cajas equivalentes; SOLO en las "
        "filas Trisemanal) y Trisemanal (float, SOLO en las filas Trisemanal: de qué trisemanal viene ese día, ej. 44). "
        "Ya excluye las filas duplicadas sin kilos, así que puedes sumar Bultos y KgsRecepcionados directo. "
        "TRISEMANAL: sus filas traen UN solo valor por día, el de la trisemanal más reciente, así que se suman "
        "directo, sin filtrar ni agrupar por horizonte ni por la columna Trisemanal. 'Las próximas 3 semanas' (y en "
        "general 'las próximas N semanas') = CAST(Fecha AS DATE) desde hoy hasta hoy + (N*7 - 1) días, es decir "
        "BETWEEN CAST(GETDATE() AS DATE) AND DATEADD(day, 20, CAST(GETDATE() AS DATE)) para 3 semanas; en la "
        "Trisemanal informa los kilos y las Cajas Eq (SUM(CajasEq)). CONVERSIONES: también puedes leer `conversiones`, la planilla de factores de Agua Santa "
        "por especie y envase (ver CONVERSIONES en tus instrucciones): Especie (igual que en datos), Envase, Defecto "
        "(1 = la fila a usar si no importa el envase), PesoCajaEqKg, PctExportable y PctComercial (fracciones: 0.83 = "
        "83%), PesoEnvaseKg, CajasEqPorEnvase, EnvasesPorBin, CajasEqPorPallet y PesoBinKg (NULL = sin dato). Únela "
        "por Especie y Defecto = 1 (o por Envase cuando importe), ej. LEFT JOIN conversiones c ON c.Especie = "
        "d.Especie AND c.Defecto = 1. Reglas: agrega en SQL (GROUP BY, SUM) en vez de traer filas "
        "sueltas, usa TOP para rankings, CAST(Fecha AS DATE) para fechas (compáralo con 'AAAA-MM-DD'; si comparas "
        "Fecha sin CAST usa 'AAAAMMDD', porque el servidor está en español y lee mal '2026-10-26' como datetime) y "
        "CAST(Semana AS INT) para semanas; compara "
        "fuentes con SUM(CASE WHEN [Base Origen] = '...' THEN KgsRecepcionados ELSE 0 END); NULLIF para dividir. "
)
_SQL_PACKING_COLUMNAS = (
        "[Base Origen] "
        "('Packing Planta' = embalaje REAL, 'Trisem Proceso' = proyección de proceso de corto plazo, "
        "'Estim Primavera' y 'Estim Invierno' = las mismas estimaciones de la cosecha), Especie, Variedad, "
        "[Variedad Agronomica], Productor, Grupo, Zonal, Packing (= planta), Fecha, Semana, Bultos, CajasEq "
        "(cajas equivalentes), KgsEmbalados, KgsVaciados, Categoria (CAT 1, EXTRA FANCY...; NULL si no viene) y "
        "Calibre (NULL si no viene). "
)
_SQL_PACKING_REGLAS = (
        "REGLAS de `packing`: (a) KgsEmbalados = lo que SALIÓ embalado y solo existe "
        "en 'Packing Planta'; KgsVaciados = lo que ENTRÓ al proceso y es lo único que traen las filas de "
        "estimación, así que para estimaciones de packing usa KgsVaciados. (b) El rendimiento de packing = "
        "SUM(KgsEmbalados)/NULLIF(SUM(KgsVaciados),0) sobre 'Packing Planta'. (c) Categoria viene escrita de "
        "varias formas ('CAT 1' y 'CAT1' son lo mismo): si agrupas por categoría, únelas con "
        "REPLACE(Categoria, ' ', ''). "
)
_SQL_PACKING_EDGE = "La fuente 'Packing Edge' está excluida por decisión de Agua Santa; no la menciones como disponible. "
_SQL_FIN = "Devuelve máximo 400 filas. Si da error, lee el mensaje, corrige y reintenta."

_DESCRIPCION_SQL = {
    ("datos", "packing"): (
        _SQL_INTRO + _SQL_DATOS
        + "TAMBIÉN puedes consultar la tabla `packing` (Packing_Consolidado), el EMBALAJE de la fruta: qué se "
          "procesó en planta, en qué categoría y calibre. Viene filtrada por lo que este usuario puede ver, igual "
          "que `datos`. Columnas: Temporada y TemporadaBase (igual que en datos), "
        + _SQL_PACKING_COLUMNAS + _SQL_PACKING_REGLAS
        + "(d) No mezcles kilos de `datos` (recepción) con los de `packing` como si "
          "fueran lo mismo: recepción es lo que llegó del campo y packing es lo que se procesó. (e) "
        + _SQL_PACKING_EDGE + _SQL_FIN
    ),
    ("datos",): _SQL_INTRO + _SQL_DATOS + _SQL_FIN,
    ("packing",): (
        "Consulta LIBRE de solo lectura (T-SQL de SQL Server) sobre el embalaje de la fruta: rankings, "
        "comparaciones entre temporadas, rendimiento, categorías y calibres, top N, etc. Escribe UN solo SELECT "
        "(o WITH ... SELECT) sobre la tabla `packing` (Packing_Consolidado): qué se procesó en planta, en qué "
        "categoría y calibre. Ya viene filtrada por lo que este usuario puede ver. Columnas: Temporada (int, la "
        "temporada oficial según la fecha: semana ISO 44 a 43; úsala para 'esta temporada', 'la pasada', comparar "
        "temporadas), TemporadaBase (la que trae la base; úsala solo si la piden), "
        + _SQL_PACKING_COLUMNAS + _SQL_PACKING_REGLAS + "(d) " + _SQL_PACKING_EDGE
        + "Agrega en SQL (GROUP BY, SUM) en vez de traer filas sueltas, usa TOP para rankings, CAST(Fecha AS DATE) "
          "para fechas (compáralo con 'AAAA-MM-DD'; si comparas Fecha sin CAST usa 'AAAAMMDD', porque el servidor "
          "está en español y lee mal '2026-10-26' como datetime) y CAST(Semana AS INT) para semanas; NULLIF para "
          "dividir. " + _SQL_FIN
    ),
}

TOOL_CONSULTAR_SQL = {
    "name": "consultar_sql",
    "description": _DESCRIPCION_SQL[("datos", "packing")],
    "input_schema": {
        "type": "object",
        "properties": {
            "consulta": {"type": "string", "description": "La consulta SELECT sobre `datos` y/o `packing`."},
            "proposito": {"type": "string", "description": "En pocas palabras, qué quieres averiguar con esta consulta."},
        },
        "required": ["consulta"],
    },
}

def _lista_tablas(tablas, union):
    """'`datos`, `packing` y/o `embarques`', para las descripciones de las herramientas."""
    tablas = [f"`{t}`" for t in tablas]
    return tablas[0] if len(tablas) == 1 else f"{', '.join(tablas[:-1])} {union} {tablas[-1]}"

TOOL_GENERAR_INFORME = {
    "name": "generar_informe",
    "description": (
        "Arma y ENVÍA un informe A MEDIDA en PDF o Excel cuando el usuario pide algo que no es el informe estándar "
        "de consultar_cosecha_flexible (estimado vs real) ni el de generar_informe_proyeccion: varias secciones, "
        "comparaciones entre temporadas, rankings, una tabla con semanas o fechas como columnas, un gráfico "
        "específico, cifras clave a elección, un comentario o análisis, etc. Cada sección es una consulta SQL sobre "
        "<TABLAS> (mismas columnas y reglas que consultar_sql) que se ejecuta al armar el archivo, así las cifras "
        "salen siempre de la base. Por defecto la primera sección lleva un gráfico pequeño de totales; la fila "
        "TOTAL se agrega sola y suma solo las cantidades (no los %, ni las columnas por unidad como USD/caja eq o "
        "promedios, que no se pueden sumar). Si necesitas un total correcto para esas columnas, haz que la "
        "consulta traiga su propia fila con 'TOTAL' en la primera columna de texto (UNION ALL): el informe la usa "
        "como fila de total y la deja fuera del gráfico, así que NO hace falta quitar el gráfico por eso. "
        "Antes de llamarla conviene probar las consultas con consultar_sql si no estás seguro "
        "de que funcionen."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "formato": {"type": "string", "enum": ["pdf", "excel"], "description": "'pdf' por defecto; 'excel' si lo quiere para trabajar los datos."},
            "titulo": {"type": "string", "description": "Título del informe."},
            "subtitulo": {"type": "string", "description": "Periodo, filtros y unidad (ej. 'Temporada 2027 · kilos · zonal Joaquin Llano')."},
            "resumen": RESUMEN_INFORME_SCHEMA,
            "consulta_kpis": {"type": "string", "description": "Opcional: SELECT sobre <TABLAS> que devuelve UNA fila; cada columna es una cifra clave del encabezado (el alias de la columna es su etiqueta, ej. AS [Kilos reales]). Usa '%' en el alias para porcentajes."},
            "secciones": {
                "type": "array",
                "description": "Secciones del informe, en orden (máximo 8).",
                "items": {
                    "type": "object",
                    "properties": {
                        "titulo": {"type": "string"},
                        "consulta_sql": {"type": "string", "description": "SELECT sobre <TABLAS> con las columnas de la tabla, ya con alias legibles (ej. AS [Kilos real])."},
                        "pivote": {
                            "type": "object",
                            "description": "Opcional, para poner valores como columnas (ej. las semanas): la consulta debe traer las columnas de filas, la columna a pivotear y el valor.",
                            "properties": {
                                "filas": {"type": "array", "items": {"type": "string"}, "description": "Columnas que quedan como filas."},
                                "columnas": {"type": "string", "description": "Columna cuyos valores pasan a ser columnas (ej. 'Semana')."},
                                "valor": {"type": "string", "description": "Columna numérica a sumar en cada celda."},
                            },
                            "required": ["filas", "columnas", "valor"],
                        },
                        "grafico": {
                            "type": "object",
                            "description": "Opcional. Si se omite, solo la primera sección lleva el gráfico de totales por defecto.",
                            "properties": {
                                "tipo": {"type": "string", "enum": ["barras", "barras_horizontales", "lineas", "torta", "ninguno"]},
                                "categoria": {"type": "string", "description": "Columna para el eje (por defecto la primera de texto)."},
                                "valores": {"type": "array", "items": {"type": "string"}, "description": "Columnas numéricas a graficar (máx. 3, todas de la MISMA unidad: kilos con kilos, USD con USD; el gráfico tiene un solo eje). Por defecto, la primera columna de cantidades y las de su misma unidad."},
                                "titulo": {"type": "string"},
                            },
                        },
                        "total": {"type": "boolean", "description": "Agregar fila TOTAL (por defecto sí)."},
                        "nota": {"type": "string", "description": "Nota breve bajo la tabla."},
                    },
                    "required": ["titulo", "consulta_sql"],
                },
            },
        },
        "required": ["formato", "titulo", "secciones"],
    },
}

# Lo que se agrega a las herramientas de SQL para los perfiles que ven embarques
# (ROLES_EMBARQUES). Para el resto esas tablas no existen: ni se describen ni se pueden leer.
DESCRIPCION_EMBARQUES = (
    " ADEMÁS, este usuario puede consultar dos tablas de EMBARQUES (exportación), con TODOS los productores "
    "(no tienen filtro de fundos). Cada fila es un pallet o parte de un pallet embarcado. "
    "`embarques` (vista Reporte_Embarque_PBI): Temporada (int, temporada de embarque), Especie (inglés), Variedad, "
    "TipoVariedad, Cajas (cajas físicas), CajasEq (cajas equivalentes), Pallets (fracción de pallet; suma para "
    "contar pallets), CajasPorPallet, Contenedor (NULL en aéreos, terrestres y naves convencionales), Recibidor "
    "(cliente), Nave, Via ('Marítima', 'Aérea', 'Terrestre'), FechaZarpe (date), SemanaZarpe (int, ISO), Puerto "
    "(de llegada), ETA (date), Mercado (región: LEJANO ORIENTE, USA EAST COAST, USA WEST COAST, EUROPA, "
    "SUDAMERICA, CENTROAMERICA, NORTEAMERICA, CHILE...), PaisVenta (casi siempre NULL). "
    "`embarques_fob` (vista Embarque-Liquidacion-PBI, las MISMAS filas con datos comerciales; no trae "
    "contenedor ni pallets): Temporada, Productor, Grupo, Zonal (calculada: zonal responsable del productor), Planta, Especie, "
    "Variedad, TipoVariedad, Serie (calibre/serie equivalente), Cajas, CajasEq, FOB (USD, monto total de la "
    "fila), FOBPool (USD, FOB del pool), Estado ('Liquidación' = liquidado, 'Factura' = solo facturado, valor "
    "provisorio), Arribado ('Sí'/'No'), Recibidor, CodigoRecibidor, Nave, Via, FechaZarpe, SemanaZarpe, Puerto, "
    "PaisLlegada (siempre informado), ETA, FechaLlegada, Mercado. Serie es el CALIBRE de lo embarcado (XL, J, "
    "JJ, JJJ, G...). En embarques NO hay categoría: la categoría solo existe en packing (lo embalado en planta, que "
    "no es lo mismo que lo embarcado). "
    "REGLAS DE AGUA SANTA para embarques: (1) 'cajas' sin más = cajas FÍSICAS (Cajas); las equivalentes solo si "
    "las piden. (2) Cuando hables de FOB, precio o retorno muestra SIEMPRE los dos: FOB y FOB pool. (3) USD por "
    "caja = SUM(FOB)/SUM(CajasEq), es decir por caja EQUIVALENTE (y lo mismo con FOBPool); dilo así: 'USD/caja "
    "eq'. (4) Los totales de FOB incluyen lo facturado sin liquidar, pero di siempre cuánto corresponde a "
    "Estado = 'Factura' (es provisorio). (5) 'Destino' o 'mercado' = Mercado (región) y Puerto; el país sale de "
    "PaisLlegada en embarques_fob. (6) Hay filas con FOB negativo (notas de crédito y ajustes): se suman igual. "
    "(7) Para cruzar embarques con cosecha usa Especie/Variedad/Temporada; no hay kilos embarcados, así que no "
    "compares kilos cosechados con cajas embarcadas como si fueran la misma unidad."
)

# Para quien ve embarques pero no el FOB: solo la tabla `embarques` y las reglas que aplican.
DESCRIPCION_EMBARQUES_SIN_FOB = (
    " ADEMÁS, este usuario puede consultar la tabla de EMBARQUES (exportación), con TODOS los productores "
    "(no tiene filtro de fundos). Cada fila es un pallet o parte de un pallet embarcado. "
    "`embarques` (vista Reporte_Embarque_PBI): Temporada (int, temporada de embarque), Especie (inglés), Variedad, "
    "TipoVariedad, Cajas (cajas físicas), CajasEq (cajas equivalentes), Pallets (fracción de pallet; suma para "
    "contar pallets), CajasPorPallet, Contenedor (NULL en aéreos, terrestres y naves convencionales), Recibidor "
    "(cliente), Nave, Via ('Marítima', 'Aérea', 'Terrestre'), FechaZarpe (date), SemanaZarpe (int, ISO), Puerto "
    "(de llegada), ETA (date), Mercado (región: LEJANO ORIENTE, USA EAST COAST, USA WEST COAST, EUROPA, "
    "SUDAMERICA, CENTROAMERICA, NORTEAMERICA, CHILE...), PaisVenta (casi siempre NULL). "
    "`embarques_detalle` (vista Embarque-Liquidacion-PBI, las MISMAS filas con productor, planta y calibre; no trae "
    "contenedor ni pallets): Temporada, Productor, Grupo, Zonal (calculada, igual que en datos), Planta, Especie, "
    "Variedad, TipoVariedad, Serie (el CALIBRE de lo embarcado: XL, J, JJ, JJJ, G...), Cajas, CajasEq, Arribado "
    "('Sí'/'No'), Recibidor, CodigoRecibidor, Nave, Via, FechaZarpe, SemanaZarpe, Puerto, PaisLlegada, ETA, "
    "FechaLlegada, Mercado. Para productor, planta o calibre usa embarques_detalle. En embarques NO hay categoría: "
    "la categoría solo existe en packing (lo embalado en planta, que no es lo mismo que lo embarcado). "
    "Este usuario NO tiene habilitados los valores FOB, precios, retornos ni liquidaciones (no existe "
    "`embarques_fob` para él). REGLAS DE AGUA SANTA para embarques: (1) 'cajas' sin más = cajas FÍSICAS (Cajas); "
    "las equivalentes solo si las piden. (2) 'Destino' o 'mercado' = Mercado (región) y Puerto. (3) Para cruzar "
    "embarques con cosecha usa Especie/Variedad/Temporada; no hay kilos embarcados, así que no compares kilos "
    "cosechados con cajas embarcadas como si fueran la misma unidad."
)
_SQL_POR_PERMISO = {}

def _herramienta_sql(nivel, bases=("datos", "packing")):
    """consultar_sql según lo que puede leer el usuario. Se arma una vez por combinación, así la
    definición es siempre la misma (la caché de prompts depende de eso)."""
    clave = (nivel, bases)
    if clave not in _SQL_POR_PERMISO:
        extra = DESCRIPCION_EMBARQUES if nivel >= 2 else DESCRIPCION_EMBARQUES_SIN_FOB if nivel == 1 else ""
        tablas = [*bases, *_tablas_embarques(nivel)]
        _SQL_POR_PERMISO[clave] = {
            **TOOL_CONSULTAR_SQL,
            "description": _DESCRIPCION_SQL[bases] + extra,
            "input_schema": {
                **TOOL_CONSULTAR_SQL["input_schema"],
                "properties": {
                    **TOOL_CONSULTAR_SQL["input_schema"]["properties"],
                    "consulta": {"type": "string", "description": f"La consulta SELECT sobre {_lista_tablas(tablas, 'y/o')}."},
                },
            },
        }
    return _SQL_POR_PERMISO[clave]

TOOL_CONSULTAR_CONFIRMACIONES = {
    "name": "consultar_confirmaciones",
    "description": "Consulta el HISTORIAL de confirmaciones y ajustes de proyección que registra el Asistente: cuestionarios enviados, confirmaciones, ajustes, correcciones, anulaciones, reportes espontáneos y resúmenes de cierre enviados a las plantas. Úsala para responder o armar informes sobre quién confirmó, quién ajustó, quién no respondió, qué se le avisó a cada planta, etc. Es la base propia del bot en SQLite (NO es SQL Server): usa sintaxis SQLite (date('now','localtime'), strftime, LIMIT en vez de TOP, || para concatenar, sin corchetes). Tabla `confirmaciones`, una fila por hecho; nada se pisa ni se borra. Columnas: id; fecha_hora (texto 'AAAA-MM-DD HH:MM:SS', hora de Chile); fecha (día de la cosecha al que se refiere, 'AAAA-MM-DD'); evento ('enviado' = se le envió el cuestionario; 'confirmado' = confirmó lo previsto tal cual; 'ajustado' = respondió con un ajuste; 'corregido' = cambió su respuesta anterior por un ajuste nuevo, que la reemplaza; 'anulado' = había ajustado y volvió a lo previsto, sin cambios (detalle = lo que quedó sin efecto); 'reporte' = avisó un ajuste sin tener un cuestionario abierto; 'resumen_planta' = se le envió a una planta el resumen de cierre); turno ('diario_am' = cuestionario de las 08:00, 'diario_pm' = confirmación de las 15:00, 'espontaneo', 'cierre_planta'; filas antiguas pueden decir 'manana'); numero; nombre; perfil (zonal, productor, planta, eas...); zonal (zonal al que está vinculado el número); productores (los de la tabla del cuestionario, separados por '; '); plantas (en resumen_planta); kg_previstos (kilos previstos de ese cuestionario o resumen); detalle (el ajuste tal como lo resumió el Asistente; en resumen_planta, cuántos productores quedaron confirmados, con ajuste o sin confirmar); texto_usuario (lo que escribió la persona); avisados (a cuántas personas se les avisó); cuestionario_id. El ÚLTIMO estado de un cuestionario es su evento más reciente por cuestionario_id; 'sin respuesta' = cuestionario con evento 'enviado' y ningún otro evento con ese cuestionario_id. El detalle completo (correcciones y anulaciones) se guarda desde el 02-11-2026; lo anterior se cargó con la última respuesta de cada cuestionario. Para un informe en PDF o Excel usa generar_informe con fuente='confirmaciones' en la sección.",
    "input_schema": {
        "type": "object",
        "properties": {
            "consulta": {"type": "string", "description": "Consulta SELECT (SQLite) sobre la tabla `confirmaciones`."},
        },
        "required": ["consulta"],
    },
}

TOOL_HISTORIAL_CONFIRMACIONES = {
    "name": "historial_confirmaciones",
    "description": (
        "Muestra, en orden y en una línea por hecho, las confirmaciones, ajustes, correcciones y anulaciones "
        "que han enviado zonales y productores (lo mismo que se le avisó al equipo). Úsala cuando pidan 'el "
        "historial', 'historial de Pedro', 'qué ha cambiado', 'todo lo que confirmó o corrigió', normalmente "
        "después de recibir un aviso de ajuste. Por defecto los últimos 7 días. Cada usuario ve solo lo de sus "
        "fundos (admin, EAS y gerencia ven todo). Entrega el resultado tal cual, sin reescribirlo en párrafos "
        "largos; si piden más detalle de un hecho puntual, ahí puedes explicarlo."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "persona": {"type": "string", "description": "Nombre (o parte) de quien reportó, ej. 'Pedro'. Opcional."},
            "productor": {"type": "string", "description": "Fundo o productor (nombre o parte). Opcional."},
            "desde": {"type": "string", "description": "AAAA-MM-DD. Por defecto, hace 6 días."},
            "hasta": {"type": "string", "description": "AAAA-MM-DD. Por defecto, hoy."},
            "incluir_confirmaciones": {"type": "boolean", "description": "Incluir las confirmaciones sin cambios (por defecto sí)."},
        },
    },
}

# Para los perfiles que ven el historial (los mismos que ven embarques), el informe a medida
# puede leer también `confirmaciones`: por sección (fuente) y en las cifras clave (fuente_kpis).
_INFORMES_POR_PERMISO = {}

def _herramienta_informe(nivel, confirmaciones, bases=("datos", "packing")):
    """generar_informe según lo que puede leer el usuario: sus bases, las tablas de embarques de
    su nivel y, si ve el historial, la fuente 'confirmaciones'. Se arma una vez por combinación,
    así la definición es siempre la misma (la caché de prompts depende de eso)."""
    clave = (nivel, bool(confirmaciones), bases)
    if clave not in _INFORMES_POR_PERMISO:
        tablas = _lista_tablas(bases, "o")
        # El valor 'cosecha' de la fuente es SQL Server, también para quien solo lee packing.
        fuente = {
            "type": "string", "enum": ["cosecha", "confirmaciones"],
            "description": f"'cosecha' (por defecto): la consulta es SQL Server sobre {tablas} y las tablas de embarques. "
                           "'confirmaciones': la consulta es SQLite sobre la tabla `confirmaciones` (ver consultar_confirmaciones).",
        }
        tool = json.loads(json.dumps(TOOL_GENERAR_INFORME).replace("<TABLAS>", tablas))
        if nivel >= 2:
            tool["description"] += " Las secciones también pueden leer `embarques` y `embarques_fob` (ver consultar_sql)."
        elif nivel == 1:
            tool["description"] += (" Las secciones también pueden leer `embarques` y `embarques_detalle` (ver "
                                    "consultar_sql); los valores FOB no están habilitados para este usuario.")
        if confirmaciones:
            tool["description"] += (" Con fuente='confirmaciones' una sección (o las cifras clave, con fuente_kpis) "
                                    "lee el historial de confirmaciones y ajustes en vez de SQL Server.")
            tool["input_schema"]["properties"]["fuente_kpis"] = fuente
            tool["input_schema"]["properties"]["secciones"]["items"]["properties"]["fuente"] = fuente
        _INFORMES_POR_PERMISO[clave] = tool
    return _INFORMES_POR_PERMISO[clave]

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
                "description": "Obligatorio si resultado='ajustado': cómo queda el ajuste de ESTE cuestionario (el día al que se refiere), claro y concreto, con variedad, magnitud y unidad si las dio. Ej: 'BOREAL: ~2.000 kg menos que lo proyectado'. Si no dio cifras, describe el cambio igual: 'GARCICA: cosecha atrasada aprox. una semana'. No incluyas datos de otros días ni el historial de reportes anteriores."
            },
            "aviso": {
                "type": "string",
                "description": "Obligatorio: lo que se le AVISA al equipo, en UNA o DOS líneas (máximo 250 caracteres): SOLO lo que la persona informó en ESTE mensaje, qué confirma o qué cambia (fundo o variedad, cifra y día). Ej: 'Hoy 03-11 no hay cosecha en ninguno de sus fundos (trisemanal 80.328 kg)', 'Confirma lo previsto para hoy', 'BOREAL: 2.000 kg menos el jueves'. Nunca repitas reportes anteriores ni el historial: eso lo ven pidiendo el historial."
            }
        },
        "required": ["resultado", "aviso"]
    },
}

DIAS_SEMANA = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")

def datos_numero(numero):
    """Nombre, rol y zonal de un número, para que Claude sepa con quién está hablando."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        fila = conn.execute(
            "SELECT nombre, rol, zonal FROM numeros_permitidos WHERE numero = ?",
            (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return dict(fila) if fila else {}
    except Exception as e:
        logger.error(f"Error obteniendo datos del número: {str(e)}")
        return {}

def _texto_zonales():
    por_zonal = {}
    for productor, zonal in asignacion_zonales().items():
        por_zonal.setdefault(zonal, []).append(productor)
    if not por_zonal:
        return "(todavía no hay zonales asignados)"
    return "\n".join(
        f"- {zonal}: {', '.join(sorted(productores))}"
        for zonal, productores in sorted(por_zonal.items(), key=lambda z: _normalizar_nombre(z[0]))
    )

def _prompt_estable():
    """Reglas y listas que no cambian entre un mensaje y otro (salvo al reiniciar o al
    cambiar la asignación de zonales). Va primero y marcado para caché: así cada vuelta del
    ciclo de herramientas no vuelve a pagar ni a esperar por estas miles de palabras."""
    lista_variedades = ", ".join(VARIEDADES_CONOCIDAS) or "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"
    lista_especies = ", ".join(f"{en} ({es})" for en, es in ESPECIE_TRADUCCION.items())
    lista_packings = ", ".join(PACKINGS_CONOCIDOS) or "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"
    alias_packing_texto = "; ".join(f'{real} = {" / ".join(alias)}' for real, alias in ALIAS_PACKING.items())
    lista_envases = ", ".join(ENVASES_CONOCIDOS) or "(lista no disponible por ahora)"
    lista_productores = ", ".join(PRODUCTORES_CONOCIDOS) or "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"
    lista_grupos = ", ".join(GRUPOS_CONOCIDOS) or "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"

    return f"""Eres el asistente de cosecha de Agua Santa en WhatsApp. Conversas como un chat de IA: entiendes
preguntas abiertas y de seguimiento, mantienes el hilo de la conversación, encadenas las consultas que
hagan falta, analizas los resultados y respondes de forma clara y útil. Te escriben desde el celular
personas del negocio: gerencia, el equipo EAS de estimaciones, zonales y productores.

CÓMO TRABAJAS
- Tus herramientas consultan la base de cosecha de Agua Santa (estimaciones y recepción real). Puedes
  llamar varias, en el orden que necesites, y ver sus resultados antes de responder: por ejemplo sacar el
  total, después mirar el detalle de lo que llama la atención, y recién entonces contestar.
- Toda cifra que entregues tiene que salir de una herramienta ejecutada en esta conversación. Nunca
  inventes, estimes "a ojo" ni completes datos que no consultaste. Si algo no está en la base, dilo.
- No hagas cuentas de cabeza con muchas cifras: si necesitas un total, un promedio o un porcentaje que
  no viene en el resultado, pídelo en la misma consulta (consultar_sql, con GROUP BY ... WITH ROLLUP
  o un UNION ALL con la fila de total; la forma GROUP BY ROLLUP(...) no funciona en este servidor) y
  copia el número que te devuelve. Si notas que una cifra que diste antes estaba mal, dilo y
  corrígela.
- Cuando te pidan un dato, consúltalo de nuevo aunque ya lo hayas respondido antes en la conversación:
  los datos cambian durante el día y se corrigen. El historial te sirve para entender a qué se refiere
  el usuario ("lo mismo pero por zonal", "y de Lapins?", "mándamelo en PDF"), no como fuente de cifras.
- Cuando una herramienta te devuelve una tabla formateada (entre ```), cópiala TAL CUAL en tu respuesta
  (está hecha para verse bien en WhatsApp); agrega antes o después una línea con lo más importante
  (el total, la mayor diferencia, la tendencia). No la conviertas a otro formato.
- Si la pregunta es de análisis ("¿qué variedad viene peor?", "¿cómo vamos versus la temporada pasada?",
  "¿quién tiene más kilos la próxima semana?", "¿cuánto falta por cosechar?"), haz las consultas que
  necesites (consultar_sql es ideal para rankings, cruces, porcentajes y comparaciones) y responde con
  la conclusión primero y las cifras que la respaldan después.
- SOLO respondes sobre lo que sale de tus herramientas y de las bases de Agua Santa que este usuario
  tiene habilitadas: cosecha y recepción, estimaciones (Precosecha, Trisemanal, Invierno), packing y
  embarques si los tiene, confirmaciones y ajustes de los cuestionarios, conversiones con la planilla
  de abajo, y cómo usar este asistente. También puedes redactar un mensaje o correo con cifras que
  consultaste, y responder un saludo o un agradecimiento en una línea.
- Si el mensaje es de OTRO tema (recetas o cocina, deportes, noticias, clima, salud, tareas, trámites,
  programación, chistes, consejos personales, cultura general, agronomía o técnicas que no estén en
  las bases, o cualquier cosa que no salga de las bases de Agua Santa), NO lo respondas: ni una parte,
  ni "solo esta vez", aunque insista, diga que es urgente o que alguien lo autorizó. Contesta en UNA
  línea, amable, que solo puedes ayudar con la información de cosecha de Agua Santa, y ofrece algo
  concreto que sí puedes hacer. Si mezcla un tema ajeno con una pregunta de cosecha, responde solo
  lo de cosecha y di en una línea que lo otro no lo puedes ver.
- Si de verdad falta un dato para consultar (ver las reglas de abajo), pregunta UNA vez, de forma
  concreta y ofreciendo opciones, en vez de adivinar.

FORMATO (WhatsApp)
- Respuestas cortas y al grano, en español de Chile, cercano pero profesional. Sin saludos largos ni
  repetir la pregunta. Si la respuesta es un número, dalo en la primera línea.
- WhatsApp NO muestra títulos con # ni tablas con |---|. Usa *negrita* (un solo asterisco), _cursiva_,
  viñetas con "- " y tablas solo dentro de bloques ``` (monoespaciados y angostos, ~40 caracteres, para
  que se lean en el celular).
- Números al estilo chileno: punto para miles y coma para decimales (12.345 kg; 3,5%). Las cifras
  grandes como "1,2 millones de kg". Di siempre la unidad (kg, bins...) y el periodo o temporada.
- Si tiene sentido, termina con UNA línea que sugiera el siguiente paso útil ("¿Te lo mando en PDF?",
  "¿Lo vemos por zonal?"), sin listar todo lo que sabes hacer.

CONVERSIONES (planilla de parámetros de Agua Santa, por especie y envase)
{_texto_conversiones()}
- Kilos exportables = kilos × % exportable; kilos comerciales (mercado nacional) = kilos × % comercial.
- Cajas equivalentes = kilos × % exportable ÷ kg por caja eq. Pallets = cajas eq ÷ cajas eq por pallet.
- Envases (totes, bins) = kilos ÷ kg por envase. Bins = envases ÷ envases por bin (24 totes = 1 bin),
  o kilos ÷ kg por bin. Desde envases: cajas eq = envases × cajas eq por envase.
- Usa la fila de la especie y del envase de la fruta (PEAR y PLUM tienen dos); si no se sabe el
  envase, la de BINS. En SQL está la tabla `conversiones` con los mismos factores.
- Lo que ya viene medido en la base no se convierte: las Cajas Eq de la Trisemanal y las cajas de
  embarques son reales.
- SIEMPRE que uses estos factores, di en una línea cuáles usaste (especie, envase y cada factor, ej.
  "Cereza en totes: 83% exportable, 5,0 kg por caja eq, 184 cajas eq por pallet") y termina pidiendo
  que los confirme o te dé otros. Si te da otros, úsalos en lo que sigue de la conversación y dilo. Si
  un factor está en blanco para esa especie (—), dilo y pídele el dato: no lo inventes.

INFORMES EN PDF O EXCEL
- Los archivos que envías son informes: título y periodo, cifras clave, un gráfico pequeño de totales
  (por defecto), la tabla completa con su fila TOTAL y la nota de fuentes. El archivo le llega solo por
  WhatsApp, así que no pegues además la tabla en el mensaje.
- Estimado vs real con alguna agrupación -> consultar_cosecha_flexible con formato_archivo.
  Proyección de las próximas semanas -> generar_informe_proyeccion. Cualquier otra cosa (varias
  secciones, comparar temporadas, un ranking, otra tabla con semanas o fechas como columnas, cifras
  clave a elección) -> generar_informe, con consultas SQL sobre `datos`.
- El informe queda abierto a lo que pida el usuario: otro tipo de gráfico (barras, barras horizontales,
  líneas, torta) o sobre otra dimensión, sin gráfico, más secciones, otras cifras clave, o un comentario
  o análisis en la sección Resumen (parámetro resumen, solo con cifras ya consultadas). Si pide cambios
  a un informe que ya le mandaste, vuelve a generarlo con los mismos parámetros más el cambio.
- Formato: "PDF" o "informe" -> pdf; "Excel" o "planilla" -> excel; si solo dice "archivo", excel.
- Después de enviar un archivo responde en 1 a 3 líneas: qué le mandaste, el dato más importante (de
  las cifras clave que te devuelve la herramienta) y, si aplica, qué más le puedes agregar.

FECHAS: usa la fecha de hoy (indicada al final) para calcular las fechas relativas que mencione el
usuario ("ayer", "hoy", "mañana", "el lunes pasado", "el 12 de agosto", "entre el 1 y el 15 de agosto",
etc.) y pásalas a las herramientas en formato YYYY-MM-DD. SIEMPRE calcula fechas concretas, nunca pases
texto literal como "hoy" o "ayer" a una herramienta. Ejemplos: "esta semana" = desde el lunes de esta
semana hasta hoy; "la semana pasada" = lunes a domingo de la semana anterior; "hasta hoy" = fecha_inicio
que corresponda y fecha_fin = hoy. Si el usuario no menciona ninguna fecha (ej. "toda la temporada",
"cuánto llevamos cosechado"), omite fecha_inicio y fecha_fin por completo en vez de inventar una fecha.

TEMPORADAS: se numeran como años (ej. 2027, 2026...). La temporada N va desde el lunes de la semana ISO
44 del año N-1 hasta el domingo de la semana 43 del año N. La temporada vigente se indica al final. Si
el usuario pregunta por "esta temporada"/"este año" no hace falta nada especial (es el comportamiento por
defecto). Si pregunta por "la temporada pasada"/"el año pasado" usa el parámetro "temporada" con la
vigente menos 1; "hace 2 temporadas" sería menos 2, y así sucesivamente. Usa "temporada" (no fechas)
salvo que el usuario también dé fechas específicas dentro de esa temporada.

También puede preguntar por una temporada FUTURA: "la próxima temporada"/"la temporada que viene" es la
vigente más 1, y así sucesivamente. Esto es válido aunque esa temporada todavía no empiece ni tenga
cosecha real: puede tener ya cargada Estim Invierno y/o Estim Primavera (la planificación de una
temporada suele empezar antes de que termine la anterior), así que no asumas que "no ha empezado"
significa que no hay nada que consultar — intenta la consulta igual. Al comienzo de una temporada es
normal que haya poco o nada de cosecha real: si una consulta de "esta temporada" sale casi vacía,
dilo y ofrece ver la temporada pasada o la proyección de las próximas semanas.

EXCEPCIÓN: consultar_cosecha_flexible NO tiene ningún periodo por defecto. Si vas a llamar esa
herramienta y el usuario dijo "esta temporada" (o cualquier referencia temporal), DEBES pasar
temporada con el número de la temporada vigente explícitamente (o las fechas que correspondan) — no lo
omitas pensando que hay un default, porque en esa herramienta omitirlo significa "el usuario no dijo
nada" y hará que se le pregunte innecesariamente.

Tienes herramientas para consultar, por variedad: estimado de temporada, cosecha real en una fecha, y
comparación de avance (estimado vs cosechado real) en una fecha. También puedes consultar resúmenes por
productor o por packing (no requieren variedad), cuándo fue la primera o la última cosecha real de una
especie/variedad/productor/packing (útil para "¿cuándo empezó la cosecha?" o "¿cuándo fue la última?"), y
el detalle de cosecha real entre un rango de fechas (agrupado por especie/variedad/productor,
opcionalmente filtrado por especie, variedad o productor). Para todo lo demás tienes consultar_sql.

MUY IMPORTANTE — FUENTE DEL "ESTIMADO": hay tres fuentes distintas de estimado en la base: Trisemanal
(rolling 3 semanas), Estim Invierno y Estim Primavera (dos ciclos de planificación de temporada). Por
defecto, SIEMPRE que hables de "estimado" o "estimación" (sin que el usuario nombre una fuente distinta),
usa Estim Primavera, que en las respuestas se llama "Precosecha" — no hace falta pasar ningún parámetro
especial, es el comportamiento por defecto de consultar_bins_estimados, comparar_estimado_vs_cosechado,
consultar_cosecha_detalle y consultar_cosecha_flexible (en consultar_sql filtra [Base Origen] = 'Estim
Primavera'). Solo si el usuario menciona explícitamente "trisemanal" o "estimación invierno"/"estimado
invierno" (una sola fuente, no una comparación), pasa fuente_estimado="trisemanal" o
fuente_estimado="invierno" en esa misma herramienta para usar esa fuente en vez de Primavera.

Además tienes consultar_comparativo_estimaciones: compara Estimación Invierno vs Estimación Primavera vs
Cosecha Real EN LA MISMA TABLA, con diferencias y %. Úsala específicamente cuando el usuario pida
COMPARAR ambas estimaciones (ej. "comparativo de estimación invierno, primavera y real", "diferencia
entre lo estimado en invierno y primavera", "cómo cambió la estimación de invierno a primavera") — no
para una consulta simple de una sola fuente, que ya cubren las herramientas normales con
fuente_estimado.

También tienes consultar_cosecha_flexible: para cuando el usuario pide una estructura o agrupación
específica (ej. "estimación de cosecha por especie", "informe con columnas fecha, estimado y real",
"total por productor", "por grupo", "por zonal", "por semana", o "solo el total sin desglose" con
agrupar_por=[]). Responde SOLO con lo que se pidió, ni más ni menos — si piden agrupar solo por especie,
no agregues variedad/productor/fecha aunque los tengas disponibles. "Por semana" (agrupar_por=['semana'])
agrupa por la semana calendario ISO estándar (la misma columna "Semana" de la base, 1-52, lunes a
domingo) en el orden natural de la temporada (44, 45, ..., 52, 1, 2, ..., 43) — la tabla muestra solo el
número de semana; no necesitas calcular ni agregar nada.

MUY IMPORTANTE — PREGUNTA LA ESTRUCTURA ANTES DE ASUMIR: cuando el usuario pida la cosecha/estimado/real
de algo (una especie, variedad, productor, packing, grupo, zonal, o una temporada) SIN indicar cómo
quiere que se resuma la respuesta —es decir, no dijo "detalle", no pidió agrupar "por especie/productor/
packing/grupo/zonal", y no especificó columnas o una estructura concreta— Y ADEMÁS el periodo es abierto
(no dio una fecha o rango acotado como "ayer", "hoy", "esta semana", "entre el X y el Y", sino que el
resultado abarcaría toda la temporada o un periodo sin acotar), NO llames a ninguna herramienta todavía.
En vez de eso, pregúntale primero cómo quiere el resumen, por ejemplo: "¿Cómo quieres que te lo resuma?
Puedo darte el total general, o desglosado por productor/fundo, por especie, por packing/planta, por
grupo, por zonal, por semana, o el detalle completo día por día." Si tampoco quedó claro el periodo
(recuerda que consultar_cosecha_flexible, a diferencia de las demás, NO tiene periodo por defecto),
agrega esa pregunta AL MISMO TIEMPO, en el mismo mensaje, para no tener que preguntar dos veces seguidas
— ej. "...y ¿para qué periodo? (esta temporada, un rango de fechas, etc.)". Esto aplica también si el
usuario responde a esta pregunta indicando SOLO la agrupación (ej. "por productor") sin mencionar el
periodo: antes de llamar a la herramienta, pregunta el periodo que falte en vez de asumirlo. Solo
después de tener ambos datos, llama a la herramienta que corresponda: consultar_cosecha_flexible con el
agrupar_por elegido (agrupar_por=[] si pide solo el total, sin desglose) y el periodo indicado, o
consultar_cosecha_detalle con detalle_por_fecha=true si pide el detalle completo por fecha. Ejemplos que
DEBEN generar esta pregunta antes de consultar: "cosecha de esta temporada", "estimación de cosecha por
especie" (sin decir período ni tenerlo de una respuesta previa), "cosecha de santina", "dame la cosecha
del grupo Valdés". Si en cambio el usuario SÍ dio una fecha o rango acotado (ej. "¿qué se cosechó ayer?",
"entre el 1 y el 15 de agosto", "esta semana"), no hace falta preguntar nada: usa
consultar_cosecha_detalle directamente, como siempre. Las preguntas puntuales de análisis o ranking
("¿qué variedad viene peor?", "¿cuál es el productor con más kilos?", "¿cuánto lleva la zona de
Joaquín?") NO necesitan esta pregunta: ya dicen qué quieren; respóndelas.

Si el usuario usa la palabra "detalle" (ej. "dame el detalle de...", "detalle de cosecha de..."), SIEMPRE
llama a consultar_cosecha_detalle con detalle_por_fecha=true, aunque pregunte solo por la cosecha en
general y no mencione fechas explícitamente — igual debe mostrarse el desglose por fecha. Usar la palabra
"detalle" ya cuenta como estructura indicada, así que en ese caso NO hace falta preguntar nada más (salvo
que falte el periodo).

ZONALES: cada productor tiene un zonal responsable, según la asignación de Agua Santa:
{_texto_zonales()}
Los productores de la base que no están en esta lista aparecen como "{ZONAL_SIN_ASIGNAR}". "La zona de
Joaquín", "los productores de Pedro", "lo de José Luis" = filtrar por ese zonal (parámetro zonal en
consultar_cosecha_flexible y generar_informe_proyeccion; columna Zonal en consultar_sql). "Por zonal" =
agrupar por zonal. Usa el nombre exacto del zonal de la lista. No confundas zonal con grupo (el grupo es
el holding dueño del fundo; un mismo grupo puede repartirse entre zonales).

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
nombre EXACTO en inglés de esta lista (ej. "mandarinas" -> "MANDARIN", "uva" -> "GRAPE"). Al responder,
nombra las especies en español.

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

INFORME DE PROYECCIÓN: cuando el usuario pida una "proyección de cosecha", un "informe de lo que viene"
o un PDF de las próximas semanas, usa generar_informe_proyeccion. ANTES de llamarla, si no fue
específico, PREGÚNTALE cómo lo quiere, ofreciéndole lo que trae por defecto para que solo confirme. Por
ejemplo: "Te lo armo en PDF con las próximas 3 semanas, en kilos de Precosecha, con una fila por fundo,
especie, variedad y planta, y un gráfico de kilos por semana. ¿Lo dejamos así, o prefieres otro
periodo, agregar la trisemanal o el real, verlo por zonal o agrupar distinto?". Si el usuario ya dijo lo
que quiere (ej. "la proyección de 4 semanas con trisemanal en Excel"), no preguntes nada y genera el
archivo directamente.

ARCHIVOS DE ESTIMADO VS REAL: si el usuario pide el resultado de una consulta como archivo
—"mándamelo en Excel", "en PDF", "un archivo con el detalle completo", "para abrirlo en la planilla"—
usa consultar_cosecha_flexible con formato_archivo="excel" o "pdf" y los filtros y agrupación que
corresponda. El archivo lleva TODAS las filas (sin el tope de la tabla del chat). Cuando una respuesta
te salga recortada por tamaño, ofrécele el archivo en vez de pedirle que acote la consulta.

CAJAS EQ (caso especial): "Caja Eq" es una unidad que solo algunas especies/variedades manejan en el
sistema (principalmente uva). Cuando el usuario pregunte por cajas eq / cajas equivalentes, usa
SIEMPRE consultar_cosecha_flexible con envase="CAJA EQ" (esa herramienta es la única que sabe
manejar este caso). Si la variedad sí tiene cajas eq registradas, te devolverá los datos normalmente.
Si NO las tiene, te devolverá una pregunta pidiendo cuántos kilos equivalen a una Caja Eq: trasládale
esa pregunta al usuario tal cual, sin inventar ni suponer un factor. Cuando el usuario responda con la
cifra (ej. "8,2 kilos", "son 10 kilos por caja"), vuelve a llamar a consultar_cosecha_flexible con los
MISMOS parámetros de la consulta original MÁS kg_por_caja_eq con ese número, y entrégale el resultado
ya convertido.

Si el usuario saluda o pide ayuda, respóndele tú directamente: breve, amable, y cuéntale en 2 o 3 líneas
lo que puedes hacer (consultar estimado y real, comparar, analizar, proyectar las próximas semanas,
mandar informes en PDF o Excel, registrar ajustes de su proyección).

Si falta la variedad, especie, productor o packing para poder consultar, pídeselo al usuario en vez de
inventarlo.

Si una palabra del mensaje NO calza exactamente con ninguna variedad/especie/packing/envase conocido,
NUNCA te rindas de inmediato ni respondas solo "no entendí" o listes TODAS las opciones disponibles.
En vez de eso: identifica cuáles 1 a 3 opciones de las listas conocidas se parecen MÁS (por escritura o,
si el mensaje viene de audio, por sonido) a lo que escribió/dijo el usuario, y pregúntale de forma breve
cuál de esas quiso decir (ej. "¿Te referías a BINS?" o "¿Es BINS, TOTES o CAJA EQ?"). Solo si de verdad
no hay ninguna opción remotamente parecida, ahí sí pide que aclare sin sugerir nada. El objetivo es que
el usuario nunca se quede sin poder avanzar la conversación."""

def _prompt_variable(es_audio=False, cuestionario_activo=None, alcance=None, numero_sender=None):
    """Lo propio de este mensaje: fecha, quién escribe, su alcance y su cuestionario."""
    hoy = date.today()
    temporada = temporada_de(hoy)
    inicio_temp, fin_temp = rango_temporada(temporada)
    partes = [
        f"HOY es {DIAS_SEMANA[hoy.weekday()]} {hoy.strftime('%Y-%m-%d')} (semana ISO "
        f"{hoy.isocalendar()[1]}). La temporada vigente es {temporada} (del "
        f"{inicio_temp.strftime('%d-%m-%Y')} al {fin_temp.strftime('%d-%m-%Y')}); la pasada es "
        f"{temporada - 1} y la próxima {temporada + 1}."
    ]

    if numero_sender:
        info = datos_numero(numero_sender)
        rol = info.get("rol") or ROL_POR_DEFECTO
        quien = f"QUIÉN TE ESCRIBE: {info.get('nombre') or 'un usuario sin nombre registrado'}, perfil {rol}"
        if info.get("zonal"):
            quien += f", zonal {info['zonal']}"
        if rol == "planta":
            quien += f", planta(s): {', '.join(plantas_de(numero_sender)) or 'ninguna asignada'}"
        partes.append(quien + ". Puedes llamarlo por su nombre de pila de vez en cuando, sin abusar.")

    if es_audio:
        partes.append(
            "ATENCIÓN: este mensaje viene de una transcripción automática de una nota de voz, puede tener "
            "errores FONÉTICOS (palabras que suenan parecido pero se transcribieron distinto, ej. \"beans\" "
            "en vez de \"bins\", \"crimen\" en vez de \"crimson\", nombres de productores o plantas mal "
            "transcritos). Ten esto muy en cuenta al interpretar el mensaje: prioriza qué palabra conocida "
            "SUENA parecido a lo transcrito, no solo cuál se escribe parecido."
        )

    if cuestionario_activo and cuestionario_activo.get("estado") == "pendiente":
        partes.append(f"""CUESTIONARIO PENDIENTE: hace poco el bot le envió a este usuario el siguiente cuestionario de
proyección de cosecha y todavía no lo responde:
---
{cuestionario_activo["mensaje"]}
---
Si el mensaje del usuario es una respuesta a ese cuestionario (confirma la proyección, o indica un
ajuste o corrección de cifras, aunque sea informal: "ok", "confirmo", "está bien", "va a ser menos",
"serán unos 10 mil kilos menos de tiffany"), usa la herramienta registrar_respuesta_cuestionario
con resultado "confirmado" o "ajustado" y el detalle del ajuste si lo hay. Si el mensaje es una
consulta normal que no tiene relación con el cuestionario, atiéndela con las demás herramientas
como siempre (el cuestionario queda pendiente; no insistas con él en cada mensaje).""")
    elif cuestionario_activo:
        respuesta_previa = (
            cuestionario_activo.get("ajuste")
            or cuestionario_activo.get("respuesta")
            or "(confirmó la proyección tal cual)"
        )
        partes.append(f"""CUESTIONARIO YA RESPONDIDO (todavía corregible): hace poco el bot le envió a este usuario el
siguiente cuestionario de proyección de cosecha:
---
{cuestionario_activo["mensaje"]}
---
El usuario ya lo respondió (estado: {cuestionario_activo["estado"]}; lo registrado: {respuesta_previa}).
Si su mensaje de ahora es una CORRECCIÓN o un nuevo ajuste sobre esa misma proyección (ej. "mejor
serán 10 mil kilos más", "me equivoqué, era garcica", "al final déjalo como estaba"), usa la
herramienta registrar_respuesta_cuestionario para ACTUALIZAR lo registrado: resultado "ajustado"
con el detalle de cómo queda el ajuste de ESE cuestionario (reemplaza al anterior, sin datos de otros
días) y en `aviso` SOLO lo que cambió en este mensaje, o "confirmado" si vuelve a la proyección
original. Si el mensaje es una consulta normal que no tiene relación con el
cuestionario, atiéndela con las demás herramientas como siempre.""")
    elif numero_sender and rol_de(numero_sender) != "planta":
        partes.append("""REPORTES DE AJUSTE: este usuario no tiene ningún cuestionario de proyección vigente ahora, pero
IGUAL puede reportar en cualquier momento que su cosecha va a venir distinta de lo proyectado
(ej. "la boreal viene con 2.000 kilos menos", "la garcica se atrasó una semana", "es solo una
corrección para avisar a los EAS"). Cuando el usuario INFORME un cambio así —no cuando pregunte
por datos— usa la herramienta registrar_respuesta_cuestionario con resultado "ajustado" y el
detalle concreto: queda registrado y se le avisa al equipo EAS. NUNCA le digas que no puedes
registrar ajustes ni que debe avisarle al EAS por otro canal: sí puedes, con esa herramienta.""")

    nivel = nivel_embarques(numero_sender)
    if nivel == 1:
        partes.append(
            "EMBARQUES: este usuario puede consultar los embarques de exportación (cajas, pallets, "
            "clientes, naves, puertos, mercados, productor, planta y calibre) con las tablas `embarques` "
            "y `embarques_detalle` de consultar_sql y generar_informe, pero NO tiene habilitados los "
            "valores FOB, precios, retornos ni "
            "liquidaciones. Si los pide, explícale con amabilidad que esa información no está "
            "habilitada para su usuario y que puede pedírsela a Edson Lazo."
        )
    elif nivel == 0 and numero_sender and rol_de(numero_sender) in ROLES_EMBARQUES:
        partes.append(
            "EMBARQUES: la información de embarques y exportación (cajas embarcadas, clientes, naves, "
            "puertos, mercados, FOB, precios o liquidaciones) no está habilitada para este usuario. "
            "Si la pide, explícale con amabilidad que no la tiene habilitada y que puede pedírsela a "
            "Edson Lazo."
        )
    elif nivel >= 2:
        partes.append(
            "EMBARQUES: este usuario puede consultar los embarques de exportación (cajas, pallets, "
            "clientes, naves, puertos, mercados y valores FOB) con las tablas `embarques` y "
            "`embarques_fob` de consultar_sql y generar_informe. Sigue las reglas de Agua Santa que "
            "vienen en la descripción de consultar_sql (cajas físicas por defecto, FOB y FOB pool "
            "juntos, USD por caja equivalente, avisar lo facturado sin liquidar)."
        )
    else:
        partes.append(
            "EMBARQUES: la información de embarques y exportación (cajas embarcadas, clientes, naves, "
            "puertos, mercados, FOB, precios o liquidaciones) es solo para los perfiles EAS y gerencia. "
            "Si este usuario la pide, explícale con amabilidad que no está disponible para su perfil."
        )

    bases = bases_de(numero_sender)
    if bases == BASES_LECTURA["cosecha"]:
        partes.append(
            "PACKING: la base de packing (el embalaje de la fruta: kilos embalados y vaciados, rendimiento de "
            "packing, categorías y calibres embalados, proyección de proceso) no está habilitada para este "
            "usuario. Si la pide, explícale con amabilidad que no la tiene habilitada y que puede pedírsela a "
            "Edson Lazo. Esto no toca la cosecha: las consultas por planta o packing de lo RECIBIDO siguen "
            "disponibles, con las herramientas de cosecha y la columna Packing de `datos`."
        )
    elif bases == BASES_LECTURA["packing"]:
        partes.append(
            "COSECHA: este usuario SOLO tiene habilitada la base de packing (el embalaje de la fruta), con la "
            "tabla `packing` de consultar_sql y generar_informe. NO tiene la base de cosecha (recepción, "
            "estimaciones de cosecha, Trisemanal, proyecciones): para él no existen la tabla `datos` ni las "
            "herramientas de cosecha, así que ignora lo que digan estas instrucciones sobre consultarlas y no "
            "ofrezcas esos datos. Si los pide, explícale con amabilidad que no tiene habilitada la cosecha y que "
            "puede pedírsela a Edson Lazo."
        )

    tablas_alcance = " y ".join(f"`{b}`" for b in bases)
    if isinstance(alcance, AlcancePlanta):
        partes.append(f"""ALCANCE DE ESTE USUARIO (perfil planta): solo puede ver lo que entra a estas plantas
(columna Packing): {", ".join(alcance) or "ninguna"}.
Las herramientas (y {tablas_alcance} en consultar_sql) ya filtran por esas plantas: todo lo que
recibas corresponde solo a ellas. Si pregunta por otra planta, explícale con naturalidad que solo
puedes darle información de la suya. Este perfil NO responde cuestionarios ni registra ajustes: si
quiere informar que la cosecha viene distinta, pídele que lo hable con el zonal del productor o con el
equipo EAS. Cada día a las {RESUMEN_PLANTAS_HORA} recibe solo el resumen de cierre: lo previsto hoy
para su planta según la Trisemanal, con lo que confirmaron o ajustaron los zonales en la tarde.""")
    elif alcance:
        partes.append(f"""ALCANCE DE ESTE USUARIO: solo tiene acceso a la información de estos fundos/productores:
{", ".join(alcance)}.
Las herramientas (y {tablas_alcance} en consultar_sql) ya filtran automáticamente por esos
fundos, así que los resultados que recibas SIEMPRE corresponden solo a ellos: no necesitas agregar
filtros ni advertir nada en cada respuesta. Si el usuario pregunta por un productor, fundo o zonal que
NO está dentro de su alcance, explícale con naturalidad que solo puedes darle información de los fundos
que tiene asignados. Nunca inventes ni estimes datos de fundos fuera de su alcance.""")

    return "\n\n".join(partes)

def construir_system_prompt(es_audio=False, cuestionario_activo=None, alcance=None, numero_sender=None):
    """System prompt en dos bloques: primero lo fijo (reglas y listas de la base), marcado
    para la caché de prompts, y después lo propio de este mensaje."""
    return [
        {"type": "text", "text": _prompt_estable(), "cache_control": {"type": "ephemeral"}},
        {"type": "text", "text": _prompt_variable(es_audio, cuestionario_activo, alcance, numero_sender)},
    ]

# Todas leen la base de cosecha, incluida consultar_resumen_packing: es lo RECIBIDO en una planta.
HERRAMIENTAS_COSECHA = {t["name"] for t in TOOLS + [TOOL_PROYECCION]}

def ejecutar_tool(tool_name, tool_input, numero_sender=None, texto_usuario=None):
    # Barrera de alcance: zonal y productor solo ven sus fundos. Se calcula aquí, desde el
    # número que escribe, y NO desde nada que venga en el mensaje o que decida el modelo.
    alcance = alcance_de(numero_sender) if numero_sender else None
    if alcance is not None and not alcance:
        return MENSAJE_SIN_PLANTA if isinstance(alcance, AlcancePlanta) else MENSAJE_SIN_FUNDOS

    # Misma barrera para las bases: no basta con no ofrecerle la herramienta al modelo.
    bases = bases_de(numero_sender)
    if tool_name in HERRAMIENTAS_COSECHA and "datos" not in bases:
        return "La base de cosecha no está habilitada para este usuario: solo puede consultar packing."

    if tool_name == "historial_confirmaciones":
        if isinstance(alcance, AlcancePlanta):
            return "El historial de reportes de los zonales no está disponible para el perfil planta."
        return historial_confirmaciones(
            numero_sender, persona=tool_input.get("persona"), productor=tool_input.get("productor"),
            desde=tool_input.get("desde"), hasta=tool_input.get("hasta"),
            incluir_confirmaciones=tool_input.get("incluir_confirmaciones", True),
        )

    if tool_name == "consultar_confirmaciones":
        if not puede_ver_confirmaciones(numero_sender):
            return "El historial de confirmaciones es solo para los perfiles admin, EAS y gerencia."
        return consultar_confirmaciones(tool_input.get("consulta"))

    if tool_name == "registrar_respuesta_cuestionario":
        if isinstance(alcance, AlcancePlanta):
            return ("Tu perfil es de planta: no registra ajustes. Si la cosecha viene distinta, "
                    "avísale al zonal del productor o al equipo EAS.")
        return registrar_respuesta_cuestionario(
            numero_sender,
            tool_input.get("resultado", "confirmado"),
            detalle_ajuste=tool_input.get("detalle_ajuste"),
            texto_usuario=texto_usuario,
            aviso=tool_input.get("aviso"),
        )

    if tool_name == "consultar_sql":
        return consultar_sql(tool_input.get("consulta"), alcance,
                             embarques=nivel_embarques(numero_sender), bases=bases)

    if tool_name == "generar_informe":
        return generar_informe_personalizado(
            numero_sender,
            tool_input.get("formato") or "pdf",
            tool_input.get("titulo"),
            tool_input.get("secciones") or [],
            subtitulo=tool_input.get("subtitulo") or None,
            resumen=tool_input.get("resumen") or None,
            consulta_kpis=tool_input.get("consulta_kpis") or None,
            alcance=alcance,
            embarques=nivel_embarques(numero_sender),
            confirmaciones=puede_ver_confirmaciones(numero_sender),
            fuente_kpis=tool_input.get("fuente_kpis") or None,
            bases=bases,
        )

    if tool_name == "generar_informe_proyeccion":
        return generar_informe_proyeccion(
            numero_destino=numero_sender,
            semanas=tool_input.get("semanas"),
            fecha_inicio=tool_input.get("fecha_inicio") or None,
            fuentes=tool_input.get("fuentes") or None,
            agrupar_por=tool_input.get("agrupar_por") or None,
            formato=tool_input.get("formato") or "pdf",
            especie=tool_input.get("especie") or None,
            variedad=normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None,
            productor=tool_input.get("productor") or None,
            packing=tool_input.get("packing") or None,
            grupo=tool_input.get("grupo") or None,
            alcance=alcance,
            zonal=tool_input.get("zonal") or None,
            grafico=tool_input.get("grafico") or None,
            resumen=tool_input.get("resumen") or None,
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
            formato_archivo=tool_input.get("formato_archivo") or None,
            numero_destino=numero_sender,
            zonal=tool_input.get("zonal") or None,
            grafico=tool_input.get("grafico") or None,
            resumen=tool_input.get("resumen") or None,
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

# Extracto del resultado de cada herramienta que se guarda en el historial: suficiente para
# que Claude sepa qué se consultó y qué salió, sin arrastrar tablas enteras a cada mensaje.
LIMITE_RESULTADO_HISTORIAL = 1500

def herramientas_disponibles(numero_sender=None):
    # La herramienta de reporte va siempre que haya número: un ajuste de proyección se avisa
    # cuando pasa, no solo mientras hay un cuestionario vigente.
    nivel = nivel_embarques(numero_sender)
    confirmaciones = puede_ver_confirmaciones(numero_sender)
    bases = bases_de(numero_sender)
    sql = [_herramienta_sql(nivel, bases), _herramienta_informe(nivel, confirmaciones, bases)]
    if confirmaciones:
        sql.append(TOOL_CONSULTAR_CONFIRMACIONES)
    # La planta solo consulta: los ajustes los reportan zonales y productores, y el historial de
    # reportes es de ellos y del equipo.
    reporta = not numero_sender or rol_de(numero_sender) != "planta"
    cosecha = TOOLS + [TOOL_PROYECCION] if "datos" in bases else []
    return (cosecha + sql
            + ([TOOL_REGISTRAR_CUESTIONARIO] if numero_sender and reporta else [])
            + ([TOOL_HISTORIAL_CONFIRMACIONES] if reporta else []))

def _historial_como_mensajes(numero_sender, nombres_herramientas):
    """Reconstruye la conversación reciente como mensajes para Claude, incluidas las
    herramientas que usó en cada respuesta (con un extracto de su resultado). Así entiende
    los seguimientos ("mándamelo en PDF", "y por zonal?") igual que un chat de IA."""
    mensajes = []
    for n, turno in enumerate(obtener_historial_conversacion(numero_sender)):
        mensajes.append({"role": "user", "content": turno["mensaje"] or "(mensaje vacío)"})
        try:
            rondas = json.loads(turno["herramientas"]) if turno["herramientas"] else []
        except (TypeError, ValueError):
            rondas = []
        for r, ronda in enumerate(rondas):
            ronda = [h for h in ronda if h.get("nombre") in nombres_herramientas]
            if not ronda:
                continue
            ids = [f"hist_{n}_{r}_{k}" for k in range(len(ronda))]
            mensajes.append({"role": "assistant", "content": [
                {"type": "tool_use", "id": id_, "name": h["nombre"], "input": h.get("entrada") or {}}
                for id_, h in zip(ids, ronda)
            ]})
            mensajes.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": id_, "content": str(h.get("resultado") or "(sin resultado)")}
                for id_, h in zip(ids, ronda)
            ]})
        mensajes.append({"role": "assistant", "content": turno["respuesta"] or "(sin respuesta)"})
    return mensajes

def _llamar_claude(system, tools, messages, sin_herramientas=False):
    """Una llamada a la API de Claude. Se lee el JSON crudo de la respuesta en vez de los
    objetos del SDK: la versión instalada (0.69) no reconoce los tipos de bloque nuevos y
    con Python 3.14 se cae al encontrar uno; con el JSON crudo, además, los bloques de
    razonamiento vuelven a la API exactamente como llegaron, que es lo que exige el ciclo de
    herramientas."""
    cuerpo_extra = {
        "output_config": {"effort": CLAUDE_ESFUERZO},
        # Caché automática del historial: cada vuelta del ciclo reutiliza lo ya enviado.
        "cache_control": {"type": "ephemeral"},
    }
    cabeceras = {}
    if CLAUDE_MODELO in MODELOS_CON_FALLBACK:
        cuerpo_extra["fallbacks"] = "default"
        cabeceras["anthropic-beta"] = BETA_FALLBACK
    parametros = dict(
        model=CLAUDE_MODELO,
        max_tokens=16000,
        system=system,
        tools=tools,
        messages=messages,
        extra_body=cuerpo_extra,
        extra_headers=cabeceras,
    )
    if sin_herramientas:
        parametros["tool_choice"] = {"type": "none"}
    crudo = claude_client.messages.with_raw_response.create(**parametros)
    datos = crudo.http_response.json()
    uso = datos.get("usage") or {}
    logger.info(
        f"Claude {datos.get('model')}: entrada {uso.get('input_tokens')} "
        f"(+{uso.get('cache_read_input_tokens') or 0} leídos de caché, "
        f"{uso.get('cache_creation_input_tokens') or 0} guardados), salida {uso.get('output_tokens')}, "
        f"fin={datos.get('stop_reason')}"
    )
    return datos

def procesar_mensaje(texto_mensaje, numero_sender=None, es_audio=False, traza=None):
    """
    Responde un mensaje como un chat de IA: Claude puede llamar herramientas, ver sus
    resultados y seguir consultando (hasta MAX_PASOS_HERRAMIENTAS rondas) antes de escribir
    la respuesta final, que redacta él con los datos obtenidos. Antes, el resultado de la
    primera herramienta se mandaba tal cual al usuario y Claude nunca lo veía, así que no
    podía analizar, comparar ni corregir una consulta.
    Si se da numero_sender, incluye la conversación reciente de ese número como contexto.
    Si es_audio=True, el mensaje viene de una transcripción de voz y puede tener errores
    fonéticos (ej. "beans" en vez de "bins").
    traza: lista opcional donde se anotan las rondas de herramientas usadas (nombre,
    parámetros y extracto del resultado), para guardarlas en el historial.
    """
    try:
        tools = herramientas_disponibles(numero_sender)
        nombres = {t["name"] for t in tools}
        messages = _historial_como_mensajes(numero_sender, nombres) if numero_sender else []
        messages.append({"role": "user", "content": texto_mensaje or "(mensaje vacío)"})

        cuestionario_activo = obtener_cuestionario_activo(numero_sender) if numero_sender else None
        system = construir_system_prompt(
            es_audio,
            cuestionario_activo=cuestionario_activo,
            alcance=alcance_de(numero_sender) if numero_sender else None,
            numero_sender=numero_sender,
        )
        pide_detalle = "detalle" in (texto_mensaje or "").lower()

        for paso in range(MAX_PASOS_HERRAMIENTAS + 1):
            ultimo = paso == MAX_PASOS_HERRAMIENTAS
            respuesta = _llamar_claude(system, tools, messages, sin_herramientas=ultimo)
            contenido = respuesta.get("content") or []
            motivo = respuesta.get("stop_reason")
            usos = [b for b in contenido if b.get("type") == "tool_use"]

            if motivo == "tool_use" and usos and not ultimo:
                messages.append({"role": "assistant", "content": contenido})
                resultados, ronda = [], []
                for uso in usos:
                    entrada = dict(uso.get("input") or {})
                    # Si el usuario pidió "detalle" explícitamente, se fuerza el desglose
                    # por fecha sin depender de que Claude lo haya marcado ("sí o sí").
                    if uso.get("name") == "consultar_cosecha_detalle" and pide_detalle:
                        entrada["detalle_por_fecha"] = True
                    logger.info(f"Herramienta {uso.get('name')} con {json.dumps(entrada, ensure_ascii=False, default=str)[:500]}")
                    try:
                        salida = ejecutar_tool(uso.get("name"), entrada, numero_sender=numero_sender,
                                               texto_usuario=texto_mensaje)
                        es_error = False
                    except Exception as e:
                        logger.exception(f"Error en la herramienta {uso.get('name')}: {str(e)}")
                        salida, es_error = f"ERROR al ejecutar la herramienta: {str(e)[:300]}", True
                    salida = str(salida) if salida not in (None, "") else "(sin resultado)"
                    bloque = {"type": "tool_result", "tool_use_id": uso.get("id"), "content": salida}
                    if es_error:
                        bloque["is_error"] = True
                    resultados.append(bloque)
                    ronda.append({"nombre": uso.get("name"), "entrada": entrada,
                                  "resultado": salida[:LIMITE_RESULTADO_HISTORIAL]})
                messages.append({"role": "user", "content": resultados})
                if traza is not None:
                    traza.append(ronda)
                continue

            texto = "\n\n".join(
                b.get("text", "") for b in contenido if b.get("type") == "text"
            ).strip()
            if texto:
                return texto
            if motivo == "refusal":
                return "No puedo ayudarte con esa solicitud. Si es una consulta de cosecha, ¿me la puedes plantear de otra forma?"
            if motivo == "max_tokens":
                return "Mi respuesta quedó demasiado larga. ¿Puedes acotar un poco la consulta, o te la mando en un archivo?"
            break

        return "No entendí tu pregunta. ¿Me la puedes decir de otra forma? Puedo consultar estimado y real, comparar, proyectar y mandarte informes."
    except anthropic.RateLimitError:
        logger.error("Claude: límite de uso alcanzado")
        return "Estoy recibiendo muchas consultas en este momento. Intenta de nuevo en un minuto."
    except anthropic.APIStatusError as e:
        logger.error(f"Claude respondió {e.status_code}: {str(e)[:500]}")
        return "Tuve un problema procesando tu mensaje. Intenta de nuevo en un momento."
    except anthropic.APIConnectionError as e:
        logger.error(f"Sin conexión con Claude: {str(e)}")
        return "No pude conectarme al asistente. Intenta de nuevo en un momento."
    except Exception as e:
        logger.exception(f"Error en procesar_mensaje (Claude): {str(e)}")
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
    """True si es un rol acotado (zonal/productor) sin fundos NI acceso total: no debe
    recibir el envío. Sin esta comprobación una lista de fundos vacía no filtra nada y la
    persona recibiría lo previsto de todos los productores sin que nadie lo haya decidido."""
    rol = (destinatario.get("rol") or ROL_POR_DEFECTO)
    if rol not in ROLES_ACOTADOS:
        return False
    if destinatario.get("acceso_total"):
        return False  # se le dio acceso a todo a propósito
    return not destinatario.get("fundos")

def _fundos_para_envio(destinatario):
    """Fundos a usar al armar el mensaje de esa persona. Con acceso total se manda None
    (sin filtro) aunque tenga algunos asignados, para que el envío coincida con lo que
    puede consultar."""
    if destinatario.get("acceso_total") or (destinatario.get("rol") or ROL_POR_DEFECTO) in ROLES_VEN_TODO:
        return None
    return destinatario.get("fundos")

def _fundo_coincide(fundo_asignado, texto):
    """¿El fundo asignado corresponde a ese productor? Igual que en los filtros: si es el
    nombre exacto de un productor de la base se exige igualdad (si no, el zonal de
    "SUPERFRUIT LTDA." recibía los avisos de "SUPERFRUIT LTDA. - LOS SUSPIROS"); si no, basta
    con que aparezca dentro del nombre."""
    if not fundo_asignado or not texto:
        return False
    asignado, nombre = _normalizar_nombre(fundo_asignado), _normalizar_nombre(texto)
    if asignado == nombre:
        return True
    conocidos = _productores_conocidos_normalizados()
    # Un nombre parcial (asignado a mano, ej. "CARMELO") calza con los productores que lo contienen.
    return (asignado not in conocidos and asignado in nombre) or (nombre not in conocidos and nombre in asignado)

def registrar_evento(evento, numero, turno=None, fecha=None, productores=None, plantas=None,
                     kg=None, detalle=None, texto_usuario=None, avisados=None, cuestionario_id=None,
                     aviso=None):
    """Agrega un hecho al historial `confirmaciones` (nunca se edita ni se borra)."""
    try:
        num = normalizar_numero(numero)
        info = datos_numero(num)
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute(
            "INSERT INTO confirmaciones (fecha, evento, turno, numero, nombre, perfil, zonal, productores, "
            "plantas, kg_previstos, detalle, texto_usuario, avisados, cuestionario_id, aviso) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (str(fecha or date.today())[:10], evento, turno, num, info.get("nombre"),
             info.get("rol") or ROL_POR_DEFECTO, info.get("zonal"),
             "; ".join(productores) if productores else None, "; ".join(plantas) if plantas else None,
             kg, detalle, texto_usuario, avisados, cuestionario_id, aviso)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando el evento {evento} en el historial: {str(e)}")

def registrar_cuestionario_enviado(numero, turno, mensaje, estado="pendiente", productores=None):
    """estado 'pendiente' = la tabla ya se entregó y esperamos su respuesta.
    estado 'pendiente_plantilla' = se mandó la plantilla y la tabla queda guardada aquí
    para entregarla apenas la persona responda y se abra la ventana de 24 h."""
    conn = sqlite3.connect(DB_LOCAL_PATH)
    # Un cuestionario nuevo deja obsoleto cualquier pendiente anterior del mismo número
    conn.execute(
        "UPDATE cuestionarios SET estado = 'vencido' WHERE numero = ? "
        "AND estado IN ('pendiente', 'pendiente_plantilla')",
        (normalizar_numero(numero),)
    )
    cursor = conn.execute(
        "INSERT INTO cuestionarios (numero, turno, mensaje, estado, productores) VALUES (?, ?, ?, ?, ?)",
        (normalizar_numero(numero), turno, mensaje, estado,
         json.dumps(productores, ensure_ascii=False) if productores is not None else None)
    )
    nuevo_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return nuevo_id

def obtener_cuestionario_activo(numero):
    """Último cuestionario de las últimas 24 h para ese número, esté pendiente O ya
    respondido: mientras siga vigente, el usuario puede corregir su respuesta (ej. ajustó
    en la mañana y a las horas quiere cambiar la cifra). None si no hay ninguno vigente."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        fila = conn.execute(
            "SELECT id, turno, mensaje, estado, respuesta, ajuste, fecha_hora_envio, productores FROM cuestionarios "
            # 'pendiente_plantilla' queda fuera: esa tabla todavía no se le entregó a la
            # persona, así que no hay nada que pueda estar respondiendo.
            "WHERE numero = ? AND estado NOT IN ('vencido', 'pendiente_plantilla') "
            "AND fecha_hora_envio >= datetime('now', 'localtime', '-1 day') "
            "ORDER BY id DESC LIMIT 1",
            (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return dict(fila) if fila else None
    except Exception as e:
        logger.error(f"Error buscando cuestionario activo: {str(e)}")
        return None

def _aviso_corto(texto, largo=300):
    texto = " ".join(str(texto or "").split())
    return texto if len(texto) <= largo else texto[:largo].rsplit(" ", 1)[0] + "…"

def _ultimo_aviso(cuestionario_id):
    """Lo último que se avisó de ese cuestionario (para decir qué quedó sin efecto)."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT COALESCE(aviso, detalle) FROM confirmaciones WHERE cuestionario_id = ? "
            "AND evento IN ('ajustado', 'corregido', 'reporte') ORDER BY id DESC LIMIT 1",
            (cuestionario_id,)
        ).fetchone()
        conn.close()
        return fila[0] if fila else None
    except Exception:
        return None

def registrar_respuesta_cuestionario(numero, resultado, detalle_ajuste=None, texto_usuario=None, aviso=None):
    """Registra (o corrige) la respuesta del cuestionario vigente del número. Mientras el
    cuestionario esté dentro de sus 24 h se puede responder de nuevo: la corrección REEMPLAZA
    lo registrado y vuelve a avisar al EAS. Si había un ajuste y la persona vuelve a lo
    previsto ("déjalo como estaba"), se avisa que el ajuste quedó anulado: si no, el EAS se
    quedaba con el aviso anterior como si siguiera vigente. Cada respuesta queda además en el
    historial `confirmaciones`. La llama Claude vía la herramienta registrar_respuesta_cuestionario;
    lo que retorna se le envía al usuario."""
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
    anula = es_correccion and resultado == "confirmado" and activo.get("estado") == "ajustado"
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        if activo:
            cuestionario_id = activo["id"]
            conn.execute(
                "UPDATE cuestionarios SET estado = ?, respuesta = ?, ajuste = ?, "
                "fecha_hora_respuesta = datetime('now', 'localtime') WHERE id = ?",
                (resultado, texto_usuario, detalle_ajuste, cuestionario_id)
            )
        else:
            # Reporte fuera de cuestionario: se guarda como una fila propia, con turno
            # 'espontaneo', para que quede en el mismo historial que el resto.
            cursor = conn.execute(
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
            cuestionario_id = cursor.lastrowid
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando respuesta de cuestionario: {str(e)}")
        return "Tuve un problema registrando tu reporte. Intenta de nuevo en un momento."

    ajuste_anterior = (activo or {}).get("ajuste") or (activo or {}).get("respuesta") or "(sin detalle)"
    # El aviso dice SOLO lo que la persona informó en este mensaje (antes iba el detalle completo,
    # que al corregir arrastraba todo lo anterior y llegaba como un historial ilegible).
    aviso = _aviso_corto(aviso or detalle_ajuste or texto_usuario or "(sin detalle)")
    avisados = None
    if resultado == "ajustado":
        avisados = notificar_ajuste_cuestionario(
            numero, aviso, es_correccion=es_correccion, texto_usuario=texto_usuario,
            turno=activo["turno"] if activo else "espontaneo",
        )
    elif anula:
        anulado = _aviso_corto(_ultimo_aviso(cuestionario_id) or ajuste_anterior, 200)
        avisados = notificar_ajuste_cuestionario(
            numero, aviso, anulacion=True, texto_usuario=texto_usuario, anulado=anulado,
            turno=activo["turno"] if activo else "espontaneo",
        )
    try:
        productores = json.loads(activo["productores"]) if activo and activo.get("productores") else None
    except (TypeError, ValueError):
        productores = None
    evento = ("reporte" if es_espontaneo else "anulado" if anula
              else "corregido" if es_correccion and resultado == "ajustado" else resultado)
    registrar_evento(
        evento, numero, turno=activo["turno"] if activo else "espontaneo",
        fecha=(activo or {}).get("fecha_hora_envio") or date.today(), productores=productores,
        detalle=ajuste_anterior if anula else detalle_ajuste, texto_usuario=texto_usuario,
        avisados=avisados, cuestionario_id=cuestionario_id, aviso=aviso,
    )

    if resultado == "ajustado":
        detalle = f":\n{aviso}"
        if es_correccion:
            return f"✅ Corrección registrada{detalle}\nGracias, reemplacé lo anterior y le avisamos al equipo EAS."
        if es_espontaneo:
            return f"✅ Reporte registrado{detalle}\nGracias, quedó guardado y le avisamos al equipo EAS."
        return f"✅ Ajuste registrado{detalle}\nGracias, quedó guardado y le avisamos al equipo EAS."
    if anula:
        return ("✅ Listo, dejé la proyección como estaba (sin ajustes) y le avisé al equipo EAS que tu "
                "ajuste anterior ya no corre.")
    if es_correccion:
        return "✅ Listo, dejé la proyección registrada como confirmada (sin ajustes)."
    return "✅ Proyección confirmada, quedó registrada. ¡Gracias!"

TURNO_AVISO = {"diario_am": "cuestionario de las 08:00", "diario_pm": "confirmación de las 15:00",
               "espontaneo": "sin cuestionario"}

def notificar_ajuste_cuestionario(numero_origen, detalle, es_correccion=False, anulacion=False,
                                  texto_usuario=None, anulado=None, turno=None):
    """Aviso inmediato cuando alguien ajusta su proyección (o anula un ajuste anterior):
    siempre al admin, a gerencia y EAS si tienen las notificaciones activadas, y a los
    zonales que tengan asignado alguno de los fundos de quien reporta (un zonal solo se
    entera de lo suyo). Es corto: SOLO lo que la persona informó en este mensaje (`detalle`
    = el aviso), lo que escribió si es breve, y cómo pedir el historial. Devuelve a cuántas
    personas les llegó."""
    try:
        num = normalizar_numero(numero_origen)
        info = next((n for n in listar_numeros_permitidos() if n["numero"] == num), None)
        quien = (info.get("nombre") if info else None) or num
        fundos_origen = fundos_efectivos(num)
        hora = datetime.now().strftime("%H:%M")
        origen = TURNO_AVISO.get(turno or "", "")
        cuando = f" ({origen}, {hora})" if origen else f" ({hora})"
        if anulacion:
            lineas = [f"↩️ {quien} anuló su ajuste{cuando}: deja lo previsto sin cambios."]
            if anulado:
                lineas.append(f"Ya no corre: {anulado}")
        elif es_correccion:
            lineas = [f"📝 {quien} corrigió su reporte{cuando}:", detalle]
        else:
            lineas = [f"📝 {quien} reportó un ajuste{cuando}:", detalle]
        escrito = " ".join(str(texto_usuario or "").split())
        if escrito and len(escrito) <= 200 and escrito.lower() not in detalle.lower():
            lineas.append(f"Escribió: «{escrito}»")
        nombre_pila = (quien or "").split()[0] if quien and not quien.isdigit() else quien
        lineas.append(f"Para ver todo lo que ha confirmado o cambiado, pídeme «historial de {nombre_pila}».")
        mensaje = "\n".join(lineas)

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
        return len(aceptados)
    except Exception as e:
        logger.error(f"Error notificando ajuste de cuestionario: {str(e)}")
        return 0

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
            sql_fundos, params_fundos = _sql_fundos(fundos)
            condiciones.append(sql_fundos)
            params.extend(params_fundos)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT Productor, Especie, Envase,
                   SUM(KgsRecepcionados) as kg, SUM(Bultos) as bultos
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
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

def _totales_semana(fecha, fundos):
    """Kilos y bultos previstos en el periodo del resumen semanal, para las cifras que van
    en la plantilla de reenganche."""
    inicio = fecha or date.today()
    fin = inicio + timedelta(days=RESUMEN_SEMANAS * 7 - 1)
    filas = obtener_previsto_por_fuente(inicio, fin, fundos=fundos) or []
    # columna 5 = kg; se suman ambas fuentes por separado y se usa la mayor, que es la
    # cifra que la persona reconoce como "lo previsto".
    kg_p = sum((f[5] or 0) for f in filas if f[3] == BASE_ORIGEN_ESTIM_PRIMAVERA)
    kg_t = sum((f[5] or 0) for f in filas if f[3] == BASE_ORIGEN_TRISEMANAL)
    return max(kg_p, kg_t), 0

def _totales_previstos(fecha, fundos):
    """Total de kilos y bultos previstos ese día, para las cifras de la plantilla."""
    filas = obtener_previsto(fecha, fecha, fundos=fundos) or []
    total_kg = sum((f[3] or 0) for f in filas)
    total_bultos = sum((f[4] or 0) for f in filas)
    return total_kg, total_bultos

def encolar_mensaje_pendiente(numero, tipo, mensaje, cuestionario_id=None):
    """Guarda un mensaje que no se pudo enviar por la ventana de 24 h, para entregarlo
    apenas la persona responda a la plantilla."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute(
            "INSERT INTO mensajes_pendientes (numero, tipo, mensaje, cuestionario_id) "
            "VALUES (?, ?, ?, ?)",
            (normalizar_numero(numero), tipo, mensaje, cuestionario_id)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error encolando mensaje pendiente: {str(e)}")
        return False

def entregar_mensajes_pendientes(numero):
    """La persona nos escribió, así que la ventana de 24 h está abierta: se le entregan los
    mensajes que habían quedado en cola (del más antiguo al más nuevo). Devuelve cuántos."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        filas = conn.execute(
            "SELECT id, tipo, mensaje, cuestionario_id FROM mensajes_pendientes "
            "WHERE numero = ? AND entregado = 0 "
            "AND fecha_hora >= datetime('now', 'localtime', '-3 days') ORDER BY id",
            (normalizar_numero(numero),)
        ).fetchall()
        conn.close()
        if not filas:
            return 0
        entregados = 0
        for fila in filas:
            if not enviar_whatsapp(numero, fila["mensaje"]):
                logger.error(f"No pude entregar el mensaje en cola {fila['id']} a {numero}")
                continue
            conn = sqlite3.connect(DB_LOCAL_PATH)
            conn.execute(
                "UPDATE mensajes_pendientes SET entregado = 1, "
                "fecha_entrega = datetime('now','localtime') WHERE id = ?",
                (fila["id"],)
            )
            # Un cuestionario recién queda a la espera de respuesta cuando su tabla llegó.
            if fila["cuestionario_id"]:
                conn.execute(
                    "UPDATE cuestionarios SET estado = 'pendiente' WHERE id = ?",
                    (fila["cuestionario_id"],)
                )
            conn.commit()
            conn.close()
            entregados += 1
            logger.info(f"Entregado a {numero} el mensaje en cola de tipo '{fila['tipo']}'")
        return entregados
    except Exception as e:
        logger.error(f"Error entregando mensajes pendientes: {str(e)}")
        return 0

def enviar_o_encolar(destinatario, mensaje, tipo, kg_plantilla=0, cuestionario_id=None, descripcion=None):
    """Envía el mensaje si la ventana de 24 h está abierta; si no, manda la plantilla de
    reenganche y lo deja en cola. Devuelve 'directo', 'plantilla' o 'error'."""
    numero = destinatario["numero"]
    if ventana_24h_abierta(numero):
        return "directo" if enviar_whatsapp(numero, mensaje) else "error"
    ok = enviar_plantilla_cuestionario(
        numero, destinatario.get("nombre"), kg_plantilla,
        fundos=_fundos_para_envio(destinatario), descripcion=descripcion,
    )
    if not ok:
        return "error"
    encolar_mensaje_pendiente(numero, tipo, mensaje, cuestionario_id=cuestionario_id)
    return "plantilla"

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
    if base_desconectada():
        logger.warning(f"Base desconectada (modo mantención): no se envía el cuestionario {turno}")
        return
    try:
        fecha = fecha or date.today()
        destinatarios = destinatarios_envios(("zonal", "productor"))
        if not destinatarios:
            logger.warning("Cuestionario: no hay destinatarios (¿ningún número con el rol requerido?)")
            return {"enviados": 0, "sin_datos": 0, "errores": 0, "detalle": "sin destinatarios"}
        enviados = sin_datos = errores = omitidos = por_plantilla = 0
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
                    fundos=_fundos_para_envio(d),
                    ajuste_previo=_ajuste_registrado_hoy(d["numero"], fecha),
                )
            else:
                mensaje = construir_cuestionario_diario(
                    fecha, nombre=d.get("nombre"), fundos=_fundos_para_envio(d)
                )
            if not mensaje:
                sin_datos += 1
                logger.info(f"Cuestionario {turno}: sin previsto para {d['numero']} (fundos={d.get('fundos')})")
                continue

            filas_previsto = obtener_previsto(fecha, fecha, fundos=_fundos_para_envio(d)) or []
            kg = sum((f[3] or 0) for f in filas_previsto)
            # Los mismos productores que van en la tabla (con el mismo tope de secciones).
            productores = list(_agrupar_por_productor(filas_previsto))[:MAX_SECCIONES_CUESTIONARIO]
            abierta = ventana_24h_abierta(d["numero"])
            # El cuestionario se registra en los dos casos; si va por plantilla queda como
            # 'pendiente_plantilla' y pasa a 'pendiente' cuando la tabla se entrega.
            cid = registrar_cuestionario_enviado(
                d["numero"], turno, mensaje,
                estado="pendiente" if abierta else "pendiente_plantilla",
                productores=productores,
            )
            resultado = enviar_o_encolar(
                d, mensaje, f"cuestionario_{turno}", kg_plantilla=kg,
                cuestionario_id=None if abierta else cid,
            )
            if resultado == "directo":
                enviados += 1
            elif resultado == "plantilla":
                por_plantilla += 1
            else:
                errores += 1
            if resultado != "error":
                registrar_evento(
                    "enviado", d["numero"], turno=turno, fecha=fecha, productores=productores, kg=kg,
                    cuestionario_id=cid,
                    detalle="por plantilla: la tabla se le entrega cuando responda" if resultado == "plantilla" else None,
                )
        logger.info(
            f"Cuestionario {turno}: {enviados} directos, {por_plantilla} por plantilla, "
            f"{sin_datos} sin datos, {errores} errores, {omitidos} omitidos por no tener fundos"
        )
        return {"enviados": enviados, "por_plantilla": por_plantilla, "sin_datos": sin_datos,
                "errores": errores, "omitidos_sin_fundos": omitidos}
    except Exception as e:
        logger.error(f"Error en job de cuestionarios: {str(e)}")
        return {"error": str(e)}

def obtener_previsto_plantas(fecha, plantas=None):
    """Previsto del día (Trisemanal) por planta, productor y especie: (planta, productor,
    especie, kg, bultos). plantas=None = todas. Sin las filas de kg NULL (duplicados)."""
    conn = conectar_sql()
    if not conn:
        return None
    try:
        condiciones = ["[Base Origen] = ?", "CAST(Fecha AS DATE) = ?", "KgsRecepcionados IS NOT NULL"]
        params = [BASE_ORIGEN_CUESTIONARIO, fecha.strftime("%Y-%m-%d")]
        if plantas is not None:
            sql_plantas, params_plantas = _sql_plantas(plantas)
            condiciones.append(sql_plantas)
            params.extend(params_plantas)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT ISNULL(LTRIM(RTRIM(Packing)), '(sin planta)') AS Planta, Productor, Especie,
                   SUM(KgsRecepcionados) AS kg, SUM(Bultos) AS bultos
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
            WHERE {" AND ".join(condiciones)}
            GROUP BY ISNULL(LTRIM(RTRIM(Packing)), '(sin planta)'), Productor, Especie
            HAVING SUM(KgsRecepcionados) > 0
            ORDER BY 1, SUM(KgsRecepcionados) DESC
        """, params)
        return cursor.fetchall()
    finally:
        conn.close()

def _responsables_del_dia(fecha):
    """Quién tenía cada productor en su confirmación de la tarde de ese día y qué respondió:
    lista de {nombre, estado, ajuste, cubre(productor), especifico}. `especifico` = el número
    está acotado a sus fundos (no tiene acceso a todo): esos mandan sobre los demás."""
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.row_factory = sqlite3.Row
    filas = conn.execute(
        "SELECT c.numero, c.estado, c.ajuste, c.productores, n.nombre, n.acceso_total "
        "FROM cuestionarios c LEFT JOIN numeros_permitidos n ON n.numero = c.numero "
        "WHERE c.turno = 'diario_pm' AND date(c.fecha_hora_envio) = ? ORDER BY c.id",
        (fecha.strftime("%Y-%m-%d"),)
    ).fetchall()
    conn.close()
    responsables = []
    for f in filas:
        try:
            lista = json.loads(f["productores"]) if f["productores"] else None
        except ValueError:
            lista = None
        if lista is not None:
            exactos = set(lista)
            cubre = lambda p, exactos=exactos: p in exactos
        elif f["acceso_total"]:
            cubre = lambda p: True
        else:
            # Cuestionarios de antes de guardar sus productores: se usa el alcance del número.
            fundos = fundos_efectivos(f["numero"])
            cubre = lambda p, fundos=fundos: any(_fundo_coincide(x, p) for x in fundos)
        responsables.append({"nombre": f["nombre"] or f["numero"], "estado": f["estado"], "ajuste": f["ajuste"],
                             "cubre": cubre, "especifico": not f["acceso_total"]})
    return responsables

def _estado_productor(productor, responsables):
    """('confirmado' | 'ajustado' | 'pendiente' | 'sin_cuestionario', responsables que cuentan)."""
    candidatos = [r for r in responsables if r["cubre"](productor)]
    candidatos = [r for r in candidatos if r["especifico"]] or candidatos
    for estado in ("ajustado", "confirmado"):
        quienes = [r for r in candidatos if r["estado"] == estado]
        if quienes:
            return estado, quienes
    return ("pendiente", candidatos) if candidatos else ("sin_cuestionario", [])

GRUPOS_RESUMEN_PLANTA = (
    ("confirmado", "✅ Confirmado"),
    ("ajustado", "📝 Con ajuste"),
    ("pendiente", "⏳ Sin confirmar"),
    ("sin_cuestionario", "❔ Sin zonal que confirme"),
)

def construir_resumen_planta(fecha=None, plantas=None, nombre=None):
    """Mensaje de cierre para una planta: lo previsto hoy (Trisemanal) por productor, separado
    según lo que respondieron los zonales en la confirmación de la tarde. Devuelve (mensaje,
    kg_total, conteo) o (None, 0, None) si no hay previsto."""
    fecha = fecha or date.today()
    filas = obtener_previsto_plantas(fecha, plantas)
    if not filas:
        return None, 0, None
    responsables = _responsables_del_dia(fecha)
    por_planta = {}
    for planta, productor, especie, kg, bultos in filas:
        por_planta.setdefault(planta, {}).setdefault(productor, []).append((especie, kg or 0, bultos or 0))
    saludo = f"Hola {nombre.strip()}" if nombre and nombre.strip() else "Hola"
    lineas = [f"{saludo}, este es el cierre de hoy {DIAS_SEMANA[fecha.weekday()]} {fecha.strftime('%d-%m')}: "
              f"lo previsto para tu planta y lo que confirmaron los zonales en la tarde.", ""]
    conteo = {estado: 0 for estado, _ in GRUPOS_RESUMEN_PLANTA}
    kg_total = 0
    for planta, productores in por_planta.items():
        kg_planta = sum(kg for filas_p in productores.values() for _, kg, _ in filas_p)
        bultos_planta = sum(b for filas_p in productores.values() for _, _, b in filas_p)
        kg_total += kg_planta
        lineas.append(f"*{planta}* · previsto {formatear_kg(kg_planta)} kg · {formatear_kg(bultos_planta)} bultos")
        grupos = {}
        for productor, filas_p in productores.items():
            estado, quienes = _estado_productor(productor, responsables)
            grupos.setdefault(estado, []).append((productor, filas_p, quienes))
            conteo[estado] += 1
        for estado, titulo in GRUPOS_RESUMEN_PLANTA:
            if estado not in grupos:
                continue
            kg_grupo = sum(kg for _, filas_p, _ in grupos[estado] for _, kg, _ in filas_p)
            lineas.append(f"{titulo} ({len(grupos[estado])} · {formatear_kg(kg_grupo)} kg)")
            tabla = [f"{'Productor':<22}{'Especie':<9}{'Kilos':>9}"]
            for productor, filas_p, _ in grupos[estado]:
                for especie, kg, _ in filas_p:
                    tabla.append(f"{_truncar(productor, 21):<22}{_truncar(traducir_especie(especie), 8):<9}"
                                 f"{formatear_kg(kg):>9}")
            lineas.append("```" + "\n".join(tabla) + "```")
            if estado == "ajustado":
                vistos = set()
                for _, _, quienes in grupos[estado]:
                    for r in quienes:
                        if (r["nombre"], r["ajuste"]) not in vistos:
                            vistos.add((r["nombre"], r["ajuste"]))
                            ajuste = r["ajuste"] or "(sin detalle)"
                            if len(ajuste) > 600:
                                ajuste = ajuste[:600].rsplit(" ", 1)[0] + "…"
                            lineas.append(f"• {r['nombre']} informó: {ajuste}")
            if estado == "pendiente":
                faltan = sorted({r["nombre"] for _, _, quienes in grupos[estado] for r in quienes})
                lineas.append(f"Falta que confirme: {', '.join(faltan)}")
        lineas.append("")
    lineas.append("Las cifras son las de la Trisemanal; los ajustes son lo que informaron los zonales "
                  "y todavía no cambian la base.")
    resumen = (f"{conteo['confirmado']} confirmados, {conteo['ajustado']} con ajuste, "
               f"{conteo['pendiente']} sin confirmar, {conteo['sin_cuestionario']} sin zonal")
    return "\n".join(lineas).strip(), kg_total, resumen

def enviar_resumen_plantas(fecha=None):
    """Job de cierre (RESUMEN_PLANTAS_HORA, 18:00 por defecto): a cada usuario con perfil
    planta, lo previsto hoy para su planta con lo confirmado o ajustado en la tarde. En modo
    prueba va solo a los admin, con todas las plantas."""
    if base_desconectada():
        logger.warning("Base desconectada (modo mantención): no se envía el resumen a las plantas")
        return
    try:
        fecha = fecha or date.today()
        destinatarios = destinatarios_envios(("planta",))
        if not destinatarios:
            logger.info("Resumen a plantas: no hay números con perfil planta")
            return {"enviados": 0, "detalle": "sin destinatarios"}
        enviados = por_plantilla = sin_datos = errores = omitidos = 0
        for d in destinatarios:
            es_planta = (d.get("rol") or ROL_POR_DEFECTO) == "planta"
            plantas = plantas_de(d["numero"]) if es_planta else None
            if es_planta and not plantas:
                omitidos += 1
                logger.warning(f"Resumen a plantas: se omite {d['numero']} ({d.get('nombre')}), sin planta asignada")
                continue
            mensaje, kg, conteo = construir_resumen_planta(fecha, plantas, nombre=d.get("nombre"))
            if not mensaje:
                sin_datos += 1
                continue
            resultado = enviar_o_encolar(d, mensaje, "resumen_planta", kg_plantilla=kg,
                                         descripcion=", ".join(plantas) if plantas else "todas las plantas")
            if resultado == "directo":
                enviados += 1
            elif resultado == "plantilla":
                por_plantilla += 1
            else:
                errores += 1
            if resultado != "error":
                registrar_evento(
                    "resumen_planta", d["numero"], turno="cierre_planta", fecha=fecha,
                    plantas=plantas or ["todas"], kg=kg,
                    detalle=conteo + (" · por plantilla: se le entrega cuando responda" if resultado == "plantilla" else ""),
                )
        logger.info(f"Resumen a plantas: {enviados} directos, {por_plantilla} por plantilla, "
                    f"{sin_datos} sin datos, {errores} errores, {omitidos} sin planta")
        return {"enviados": enviados, "por_plantilla": por_plantilla, "sin_datos": sin_datos,
                "errores": errores, "omitidos_sin_planta": omitidos}
    except Exception as e:
        logger.error(f"Error en el resumen a plantas: {str(e)}")
        return {"error": str(e)}

def _lunes_de(fecha):
    return fecha - timedelta(days=fecha.weekday())

FUENTES_RESUMEN = (BASE_ORIGEN_ESTIM_PRIMAVERA, BASE_ORIGEN_TRISEMANAL)

def obtener_previsto_por_fuente(fecha_inicio, fecha_fin, fundos=None):
    """Previsto por semana, productor y especie, SEPARADO por fuente (Precosecha y
    Trisemanal), para poder mostrar las dos en la misma tabla. Se filtran las filas con
    kg NULL porque son duplicados exactos que doblarían los bultos."""
    conn = conectar_sql()
    if not conn:
        return None
    try:
        condiciones = [
            "[Base Origen] IN (?, ?)",
            "CAST(Fecha AS DATE) BETWEEN ? AND ?",
            "KgsRecepcionados IS NOT NULL",
        ]
        params = [
            FUENTES_RESUMEN[0], FUENTES_RESUMEN[1],
            fecha_inicio.strftime("%Y-%m-%d"), fecha_fin.strftime("%Y-%m-%d"),
        ]
        fundos = [f for f in (fundos or []) if f]
        if fundos:
            sql_fundos, params_fundos = _sql_fundos(fundos)
            condiciones.append(sql_fundos)
            params.extend(params_fundos)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT Productor, Especie, CAST(Semana AS INT) AS semana, [Base Origen],
                   MIN(CAST(Fecha AS DATE)) AS desde,
                   SUM(KgsRecepcionados) AS kg
            FROM {RECEPCION_SQL} AS Recepcion_Consolidada
            WHERE {" AND ".join(condiciones)}
            GROUP BY Productor, Especie, CAST(Semana AS INT), [Base Origen]
            HAVING SUM(KgsRecepcionados) > 0
        """, params)
        return cursor.fetchall()
    finally:
        conn.close()

def _tabla_dos_fuentes(datos, titulo, ancho_clave, orden=None):
    """Tabla de dos columnas de kilos (Precosecha y Trisemanal) sobre una misma clave.
    `datos` es {clave: [kg_precosecha, kg_trisemanal]}. Un guion marca la fuente que
    todavía no tiene datos cargados, que es lo normal al principio de la temporada."""
    etiqueta_p = ETIQUETA_FUENTE[BASE_ORIGEN_ESTIM_PRIMAVERA]
    etiqueta_t = ETIQUETA_FUENTE[BASE_ORIGEN_TRISEMANAL]
    # 11 y no 10: "Precosecha" mide 10, y sin ese espacio los dos encabezados se pegan.
    ancho_num = 11
    filas = [f"{titulo:<{ancho_clave}}{etiqueta_p:>{ancho_num}}{etiqueta_t:>{ancho_num}}"]
    claves = orden if orden is not None else sorted(datos, key=lambda k: datos[k][0], reverse=True)
    tot_p = tot_t = 0
    for clave in claves:
        kg_p, kg_t = datos[clave]
        tot_p += kg_p
        tot_t += kg_t
        filas.append(
            f"{_truncar(str(clave), ancho_clave):<{ancho_clave}}"
            f"{(formatear_kg(kg_p) if kg_p else '-'):>{ancho_num}}"
            f"{(formatear_kg(kg_t) if kg_t else '-'):>{ancho_num}}"
        )
    filas.append(
        f"{'TOTAL':<{ancho_clave}}"
        f"{(formatear_kg(tot_p) if tot_p else '-'):>{ancho_num}}"
        f"{(formatear_kg(tot_t) if tot_t else '-'):>{ancho_num}}"
    )
    return "\n".join(filas), tot_p, tot_t

def construir_resumen_semanal(fecha=None, nombre=None, fundos=None, para_eas=False):
    """Mensaje de los lunes 08:00: lo previsto para las próximas RESUMEN_SEMANAS semanas,
    mostrando Precosecha y Trisemanal lado a lado (la trisemanal recién aparece cuando la
    temporada avanza). Para el productor va su desglose por semana y especie; para el EAS,
    el consolidado por semana y por productor."""
    try:
        fecha = fecha or date.today()
        # Desde el día del envío (los lunes, el mismo lunes) hasta 20 días después.
        inicio = fecha
        fin = inicio + timedelta(days=RESUMEN_SEMANAS * 7 - 1)
        filas = obtener_previsto_por_fuente(inicio, fin, fundos=fundos)
        if not filas:
            return None

        # Acumuladores: [kg Precosecha, kg Trisemanal]
        por_semana, por_especie, por_productor = {}, {}, {}
        etiqueta_semana_txt = {}
        por_productor_semana = {}
        for productor, especie, semana, origen, desde, kg in filas:
            i = 0 if origen == BASE_ORIGEN_ESTIM_PRIMAVERA else 1
            kg = kg or 0
            por_semana.setdefault(semana, [0, 0])[i] += kg
            por_especie.setdefault(traducir_especie(especie), [0, 0])[i] += kg
            por_productor.setdefault(productor, [0, 0])[i] += kg
            por_productor_semana.setdefault(productor, {}).setdefault(semana, [0, 0])[i] += kg
            # Se etiqueta cada semana con el lunes de esa semana, no con la primera fecha
            # con datos, para que el rótulo no cambie según qué días traigan registros.
            if semana not in etiqueta_semana_txt and desde:
                lunes = _lunes_de(desde)
                etiqueta_semana_txt[semana] = f"S{etiqueta_semana(semana)} {lunes.strftime('%d-%m')}"

        semanas_ordenadas = sorted(por_semana, key=orden_semana)
        claves_semana = [etiqueta_semana_txt.get(s, f"S{etiqueta_semana(s)}") for s in semanas_ordenadas]
        datos_semana = {etiqueta_semana_txt.get(s, f"S{etiqueta_semana(s)}"): por_semana[s]
                        for s in semanas_ordenadas}
        rango = f"del {inicio.strftime('%d-%m')} al {fin.strftime('%d-%m')}"

        if para_eas:
            tabla_sem, tot_p, tot_t = _tabla_dos_fuentes(datos_semana, "Semana", 12, orden=claves_semana)
            tabla_prod, _, _ = _tabla_dos_fuentes(por_productor, "Productor", 20)
            return "\n".join([
                f"📅 Previsto para las próximas {RESUMEN_SEMANAS} semanas ({rango})",
                "",
                f"```{tabla_sem}```",
                "",
                f"Por productor (total {RESUMEN_SEMANAS} semanas):",
                f"```{tabla_prod}```",
                "",
                f"{len(por_productor)} productores. "
                + ("La trisemanal aún no tiene datos cargados para este periodo. "
                   if not tot_t else "")
                + "Si lo necesitas en Excel o PDF, pídemelo y te lo mando como archivo.",
            ])

        saludo = f"Hola {nombre.strip()}" if nombre and nombre.strip() else "Hola"
        tabla_sem, tot_p, tot_t = _tabla_dos_fuentes(datos_semana, "Semana", 12, orden=claves_semana)
        lineas = [
            f"{saludo}, esta es tu cosecha prevista para las próximas {RESUMEN_SEMANAS} "
            f"semanas ({rango}):",
            "",
            f"```{tabla_sem}```",
            "",
        ]
        if len(por_productor) > 1:
            # Si tiene varios fundos, se detalla cada uno por semana.
            for productor in list(por_productor)[:MAX_SECCIONES_CUESTIONARIO]:
                datos_p = {etiqueta_semana_txt.get(s, f"S{etiqueta_semana(s)}"): v
                           for s, v in por_productor_semana[productor].items()}
                orden_p = [c for c in claves_semana if c in datos_p]
                tabla_p, _, _ = _tabla_dos_fuentes(datos_p, "Semana", 12, orden=orden_p)
                lineas.append(f"Productor {productor}:")
                lineas.append(f"```{tabla_p}```")
                lineas.append("")
        tabla_esp, _, _ = _tabla_dos_fuentes(por_especie, "Especie", 12)
        lineas.append(f"Por especie (total {RESUMEN_SEMANAS} semanas):")
        lineas.append(f"```{tabla_esp}```")
        lineas.append("")
        if not tot_t:
            lineas.append("La trisemanal aún no tiene datos cargados para este periodo.")
        lineas.append("Si lo necesitas en Excel o PDF, pídemelo y te lo mando como archivo.")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error construyendo resumen semanal: {str(e)}")
        return None

def enviar_resumen_semanal(fecha=None):
    """Job de los lunes 08:00: su semana a cada productor/zonal, y el consolidado al EAS
    (todo solo a los admin mientras dure el modo prueba)."""
    if base_desconectada():
        logger.warning("Base desconectada (modo mantención): no se envía el resumen semanal")
        return
    try:
        fecha = fecha or date.today()
        enviados = sin_datos = errores = omitidos = por_plantilla = 0
        for d in destinatarios_envios(("zonal", "productor")):
            if _sin_fundos_asignados(d):
                omitidos += 1
                logger.warning(
                    f"Resumen semanal: se omite {d['numero']} ({d.get('nombre')}), "
                    f"rol {d.get('rol')} sin fundos asignados"
                )
                continue
            mensaje = construir_resumen_semanal(fecha, nombre=d.get("nombre"), fundos=_fundos_para_envio(d))
            if not mensaje:
                sin_datos += 1
                continue
            kg, _b = _totales_semana(fecha, _fundos_para_envio(d))
            resultado = enviar_o_encolar(d, mensaje, "resumen_semanal", kg_plantilla=kg)
            if resultado == "directo":
                enviados += 1
            elif resultado == "plantilla":
                por_plantilla += 1
            else:
                errores += 1

        enviados_eas = 0
        mensaje_eas = construir_resumen_semanal(fecha, para_eas=True)
        destinatarios_eas = destinatarios_envios(("eas", "admin", "gerencia"))
        if mensaje_eas:
            kg_eas, _b = _totales_semana(fecha, None)
            aceptados_eas = []
            for d in destinatarios_eas:
                resultado = enviar_o_encolar(d, mensaje_eas, "resumen_semanal", kg_plantilla=kg_eas)
                if resultado == "directo":
                    enviados_eas += 1
                    aceptados_eas.append(d)
                elif resultado == "plantilla":
                    por_plantilla += 1
                    aceptados_eas.append(d)
            if aceptados_eas:
                registrar_alerta_enviada("resumen_semanal_eas", mensaje_eas, aceptados_eas)
        logger.info(
            f"Resumen semanal: {enviados} directos a zonales, {enviados_eas} directos al EAS, "
            f"{por_plantilla} por plantilla, {sin_datos} sin datos, {omitidos} sin fundos"
        )
        return {"enviados_productores": enviados, "enviados_eas": enviados_eas,
                "por_plantilla": por_plantilla, "sin_datos": sin_datos,
                "errores": errores, "omitidos_sin_fundos": omitidos}
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
    if base_desconectada():
        logger.warning("Base desconectada (modo mantención): no se envía la alerta de desviaciones")
        return
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
        aceptados, enviados, por_plantilla = [], 0, 0
        for d in destinatarios:
            resultado = enviar_o_encolar(d, mensaje, "alerta_desviaciones")
            if resultado == "directo":
                enviados += 1
                aceptados.append(d)
            elif resultado == "plantilla":
                por_plantilla += 1
                aceptados.append(d)
        if aceptados:
            registrar_alerta_enviada("desviacion_semanal", mensaje, aceptados)
        logger.info(
            f"Alerta de desviaciones: {enviados} directos, {por_plantilla} por plantilla, "
            f"{num_desviadas} variedades fuera de rango"
        )
        return {"enviados": enviados, "por_plantilla": por_plantilla,
                "variedades_desviadas": num_desviadas}
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

def actualizar_temporada_actual():
    """La temporada vigente se calculaba solo al arrancar: si el servicio seguía corriendo al
    cruzar la semana 44, las consultas de "esta temporada" seguían en la anterior hasta el
    próximo reinicio. Se recalcula todos los días a las 00:05."""
    global TEMPORADA_ACTUAL
    nueva = obtener_temporada_actual()
    if nueva and nueva != TEMPORADA_ACTUAL:
        logger.info(f"Cambio de temporada: {TEMPORADA_ACTUAL} -> {nueva}")
        TEMPORADA_ACTUAL = nueva
    # De paso se limpian los ids de mensajes recibidos: solo sirven para descartar los
    # reintentos de Meta, que llegan dentro de pocas horas.
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.execute("DELETE FROM mensajes_recibidos WHERE fecha_hora < datetime('now', 'localtime', '-7 days')")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error limpiando mensajes recibidos: {str(e)}")

@app.on_event("startup")
def iniciar_scheduler():
    if scheduler.running:
        return
    scheduler.add_job(actualizar_temporada_actual, "cron", hour=0, minute=5, id="temporada_actual")
    h_am, m_am = _hora_cron(CUESTIONARIO_HORA_AM, (8, 0))
    h_pm, m_pm = _hora_cron(CUESTIONARIO_HORA_PM, (15, 0))
    h_sem, m_sem = _hora_cron(RESUMEN_SEMANAL_HORA, (8, 0))
    h_al, m_al = _hora_cron(ALERTA_SEMANAL_HORA, (8, 0))
    scheduler.add_job(enviar_cuestionarios, "cron", args=["diario_am"], hour=h_am, minute=m_am, id="cuestionario_am")
    scheduler.add_job(enviar_cuestionarios, "cron", args=["diario_pm"], hour=h_pm, minute=m_pm, id="cuestionario_pm")
    scheduler.add_job(enviar_resumen_semanal, "cron", day_of_week=RESUMEN_SEMANAL_DIA, hour=h_sem, minute=m_sem, id="resumen_semanal")
    scheduler.add_job(enviar_alerta_desviaciones, "cron", day_of_week=ALERTA_SEMANAL_DIA, hour=h_al, minute=m_al, id="alerta_semanal")
    h_pl, m_pl = _hora_cron(RESUMEN_PLANTAS_HORA, (18, 0))
    scheduler.add_job(enviar_resumen_plantas, "cron", hour=h_pl, minute=m_pl, id="resumen_plantas")
    scheduler.start()
    modo = "PRODUCCIÓN" if ENVIOS_AUTOMATICOS_PRODUCCION else "PRUEBA (solo números admin)"
    logger.info(
        f"Scheduler iniciado en modo {modo}: cuestionario diario {h_am:02d}:{m_am:02d} y "
        f"confirmación {h_pm:02d}:{m_pm:02d}, resumen semanal {RESUMEN_SEMANAL_DIA} {h_sem:02d}:{m_sem:02d}, "
        f"alerta desviaciones {ALERTA_SEMANAL_DIA} {h_al:02d}:{m_al:02d}, resumen a plantas {h_pl:02d}:{m_pl:02d}"
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

# Los mensajes se atienden en segundo plano: una respuesta puede encadenar varias consultas
# y tardar más de lo que Meta espera la respuesta del webhook (si no le contestamos a
# tiempo, reintenta y la persona recibía la respuesta repetida). Además, mientras se
# procesaba un mensaje dentro del webhook, el servidor completo quedaba bloqueado.
EJECUTOR_MENSAJES = ThreadPoolExecutor(
    max_workers=int(os.getenv("HILOS_MENSAJES", "8")), thread_name_prefix="mensaje"
)
_candados_por_numero = {}
_candado_general = threading.Lock()

def _candado_de(numero):
    """Un candado por número: los mensajes de una misma persona se responden en orden y
    cada uno con el historial del anterior ya guardado."""
    with _candado_general:
        return _candados_por_numero.setdefault(normalizar_numero(numero), threading.Lock())

def registrar_mensaje_recibido(wamid):
    """True si es la primera vez que llega este mensaje; False si Meta lo está reintentando."""
    if not wamid:
        return True
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        cursor = conn.execute("INSERT OR IGNORE INTO mensajes_recibidos (wamid) VALUES (?)", (wamid,))
        nuevo = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return nuevo
    except Exception as e:
        logger.error(f"Error registrando mensaje recibido: {str(e)}")
        return True

def mostrar_escribiendo(wamid):
    """Marca el mensaje como leído y muestra "escribiendo..." mientras se arma la respuesta
    (dura hasta que respondemos o 25 segundos). Si falla no importa: es solo cortesía."""
    if not wamid:
        return
    try:
        requests.post(
            f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/messages",
            headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"},
            json={"messaging_product": "whatsapp", "status": "read", "message_id": wamid,
                  "typing_indicator": {"type": "text"}},
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"No pude mostrar 'escribiendo...': {str(e)}")

def atender_mensaje(message):
    """Procesa un mensaje entrante completo: acceso, transcripción, respuesta y envío."""
    numero_sender = message.get("from")
    msg_type = message.get("type")
    try:
        if not numero_esta_permitido(numero_sender):
            logger.warning(f"Mensaje rechazado (número no autorizado): {numero_sender}")
            enviar_whatsapp(
                numero_sender,
                "No tienes acceso a este asistente. Si deberías tenerlo, contacta a quien "
                "lo administra (Edson Lazo +56954482135) para solicitar acceso."
            )
            return

        with _candado_de(numero_sender):
            mostrar_escribiendo(message.get("id"))
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
                    return
            else:
                enviar_whatsapp(
                    numero_sender,
                    "Por ahora solo puedo responder mensajes de texto o notas de voz."
                )
                return

            logger.info(f"Mensaje de {numero_sender}: {msg_text}")

            if base_desconectada():
                # Sin base no hay cifras: se avisa y no se llama a Claude (inventaría o diría
                # que no hay datos). Los cuestionarios pendientes quedan en cola para después.
                guardar_conversacion(numero_sender, msg_type, msg_text, MENSAJE_BASE_DESCONECTADA)
                enviar_whatsapp(numero_sender, MENSAJE_BASE_DESCONECTADA)
                return

            # Si tenía un cuestionario esperando (se le mandó la plantilla porque la ventana
            # de 24 h estaba cerrada), su mensaje la reabre: primero se le entrega la tabla y
            # recién después se atiende lo que escribió.
            entregar_mensajes_pendientes(numero_sender)

            traza = []
            respuesta = procesar_mensaje(msg_text, numero_sender, es_audio=(msg_type == "audio"), traza=traza)
            guardar_conversacion(numero_sender, msg_type, msg_text, respuesta, herramientas=traza)
            enviar_whatsapp(numero_sender, respuesta)
    except Exception as e:
        logger.exception(f"Error atendiendo mensaje de {numero_sender}: {str(e)}")
        enviar_whatsapp(numero_sender, "Tuve un problema procesando tu mensaje. Intenta de nuevo en un momento.")

@app.post("/webhook")
async def receive_message(request: Request):
    """
    Recibe mensajes de WhatsApp (POST request). Responde de inmediato y deja cada mensaje
    en la cola de atención (ver EJECUTOR_MENSAJES).
    """
    try:
        data = await request.json()
        logger.info(f"Webhook recibido: {data}")

        for entry in data.get("entry") or []:
            for change in entry.get("changes", []):
                value = change.get("value", {})

                # Acuses de entrega de los mensajes que enviamos (sent/delivered/read/failed).
                for status in value.get("statuses", []):
                    registrar_estado_entrega(status)

                for message in value.get("messages", []):
                    if not registrar_mensaje_recibido(message.get("id")):
                        logger.info(f"Mensaje {message.get('id')} repetido por Meta: se ignora")
                        continue
                    EJECUTOR_MENSAJES.submit(atender_mensaje, message)

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
    if base_desconectada():
        return {"status": "Bot activo, base desconectada (modo mantención)", "base": "desconectada"}
    return {"status": "Bot activo y listo", "base": "conectada"}

@app.get("/admin/base")
def admin_base(clave: str, estado: str = None):
    """
    Modo mantención de la base SQL, sin reiniciar el bot. estado=desconectar: no abre conexiones,
    responde con un aviso y no hace los envíos programados; estado=conectar: vuelve a la normalidad.
    Al conectar vuelve a leer los catálogos (variedades, productores...), que quedan vacíos si
    el bot arrancó desconectado. Sin estado, solo informa. Uso: .../admin/base?clave=...&estado=desconectar
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    if estado is not None:
        accion = estado.strip().lower()
        if accion in ("desconectar", "mantencion", "mantención", "off", "0"):
            with open(ARCHIVO_BASE_DESCONECTADA, "w", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.warning("Base desconectada por /admin/base (modo mantención)")
        elif accion in ("conectar", "on", "1"):
            if os.path.exists(ARCHIVO_BASE_DESCONECTADA):
                os.remove(ARCHIVO_BASE_DESCONECTADA)
            logger.warning("Base reconectada por /admin/base")
            return {"status": "ok", "base": "conectada", "catalogos": recargar_catalogos()}
        else:
            return JSONResponse({"status": "error", "error": "estado debe ser desconectar o conectar"},
                                status_code=400)
    return {"status": "ok", "base": "desconectada" if base_desconectada() else "conectada"}

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

# Plantillas aprobadas en Meta (WhatsApp Manager > Plantillas de mensajes). El nombre y el
# idioma deben calzar EXACTO con lo aprobado ahí: si no, Meta responde 132001 y no entrega.
# Verificado contra la API el 24-09-2026 sondeando la respuesta de validación.
PLANTILLA_BIENVENIDA_NOMBRE = os.getenv("PLANTILLA_BIENVENIDA_NOMBRE", "bienvenida")
PLANTILLA_BIENVENIDA_IDIOMA = os.getenv("PLANTILLA_BIENVENIDA_IDIOMA", "es_CL")
# La de bienvenida usa una variable CON NOMBRE ({{nombre}}); la de cuestionario usa
# variables posicionales ({{1}}, {{2}}, {{3}}).
PLANTILLA_CUESTIONARIO_NOMBRE = os.getenv("PLANTILLA_CUESTIONARIO_NOMBRE", "cuestionario")
PLANTILLA_CUESTIONARIO_IDIOMA = os.getenv("PLANTILLA_CUESTIONARIO_IDIOMA", "es_CL")

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
    return enviar_plantilla(
        numero_destino,
        PLANTILLA_BIENVENIDA_NOMBRE,
        PLANTILLA_BIENVENIDA_IDIOMA,
        [{"type": "text", "parameter_name": "nombre", "text": (nombre or "").strip() or "equipo"}],
        etiqueta="bienvenida",
    )

def enviar_plantilla(numero_destino, nombre_plantilla, idioma, parametros, etiqueta=""):
    """Envía una plantilla aprobada. A diferencia del texto libre, una plantilla SÍ llega
    fuera de la ventana de 24 h de WhatsApp (error 131047). OJO: los parámetros no pueden
    llevar saltos de línea, tabulaciones ni más de 4 espacios seguidos — Meta los rechaza con
    132018, por eso las tablas nunca pueden ir dentro de una plantilla."""
    try:
        payload = {
            "messaging_product": "whatsapp",
            "to": numero_destino,
            "type": "template",
            "template": {
                "name": nombre_plantilla,
                "language": {"code": idioma},
                "components": [{"type": "body", "parameters": parametros}],
            },
        }
        response = requests.post(
            f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/messages",
            json=payload,
            headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"},
        )
        if response.status_code != 200:
            logger.error(
                f"Plantilla '{nombre_plantilla}' ({etiqueta}) rechazada para {numero_destino}: "
                f"{response.status_code} {response.text}"
            )
            return False
        registrar_mensaje_enviado(response, numero_destino)
        logger.info(f"Plantilla '{nombre_plantilla}' ({etiqueta}) aceptada para {numero_destino}")
        return True
    except Exception as e:
        logger.error(f"Error enviando plantilla '{nombre_plantilla}': {str(e)}")
        return False

def _texto_plantilla(valor):
    """Deja un valor apto para un parámetro de plantilla: sin saltos de línea, sin tabs y
    sin rachas de espacios, que Meta rechaza (132018)."""
    texto = " ".join(str(valor or "").split())
    return texto or "-"

def _descripcion_fundos(fundos):
    """Cómo nombrar el alcance de la persona dentro de la plantilla, en una sola línea.
    None = ve todos los fundos; una lista = sus fundos asignados."""
    if not fundos:
        return "todos los fundos"
    if len(fundos) == 1:
        return str(fundos[0])
    return f"sus {len(fundos)} fundos"

def enviar_plantilla_cuestionario(numero_destino, nombre, kg, fundos=None, descripcion=None):
    """Plantilla de re-enganche del cuestionario diario. Los tres parámetros van en el orden
    del texto aprobado en Meta: "Hola {{1}}, tienes tu cuestionario de cosecha de hoy:
    {{2}} kg previstos en {{3}}." — o sea nombre, kilos y FUNDO (no bultos)."""
    return enviar_plantilla(
        numero_destino,
        PLANTILLA_CUESTIONARIO_NOMBRE,
        PLANTILLA_CUESTIONARIO_IDIOMA,
        [
            {"type": "text", "text": _texto_plantilla(nombre or "equipo")},
            {"type": "text", "text": _texto_plantilla(formatear_kg(kg))},
            {"type": "text", "text": _texto_plantilla(descripcion or _descripcion_fundos(fundos))},
        ],
        etiqueta="cuestionario",
    )

# ============================================================================
# INFORMES (PDF y Excel): cifras clave, gráfico de totales y la tabla completa
# ============================================================================
#
# Todo archivo que arma el bot es un INFORME, no una tabla suelta: título y periodo, cifras
# clave, un gráfico pequeño con los totales (por defecto), la tabla con su fila TOTAL y la
# nota de fuentes. Se describe con un dict y se dibuja igual en PDF (reportlab) y en Excel
# (openpyxl, con gráfico nativo de Excel):
#   informe = {"titulo", "subtitulo", "resumen", "kpis": [kpi], "secciones": [seccion],
#              "notas": [texto]}
#   kpi     = {"etiqueta", "valor", "tipo": "numero"|"porcentaje"|"texto", "unidad", "signo"}
#   seccion = {"titulo", "encabezados", "filas", "total" (fila o None), "grafico" (o None),
#              "nota"}
#   grafico = {"tipo": "barras"|"barras_horizontales"|"lineas"|"torta", "titulo",
#              "categorias": [...], "series": [(nombre, [valores])]}

FORMATOS_ARCHIVO = {
    "excel": ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    "pdf": ("pdf", "application/pdf"),
}

TIPOS_GRAFICO = ("barras", "barras_horizontales", "lineas", "torta")
# Paleta categórica validada para daltonismo, en orden FIJO: la primera serie (el estimado)
# es siempre azul y la segunda (el real) naranjo, en todos los informes. No se inventan
# colores: desde la 10ª categoría todo se junta en "Otros", en gris.
COLORES_SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
COLOR_OTROS = "#898781"
COLOR_MARCA = "#3f7d4f"
COLOR_TINTA = "#0b0b0b"
COLOR_TINTA_2 = "#52514e"
COLOR_TINTA_SUAVE = "#898781"
COLOR_GRILLA = "#e1e0d9"
COLOR_EJE = "#c3c2b7"
COLOR_FONDO_FILA = "#f2f4ee"
COLOR_FONDO_TOTAL = "#e3ebe0"
COLOR_FONDO_KPI = "#f3f6f1"
MAX_CATEGORIAS_GRAFICO = 10
MAX_FILAS_INFORME = int(os.getenv("MAX_FILAS_INFORME", "5000"))
NOTA_FUENTES = (
    "Precosecha = Estimación Primavera · Trisemanal = proyección de corto plazo · "
    "Real = Recepción Planta. Kilos recepcionados según la base Control EAS."
)

def _fmt_numero(valor, decimales=0):
    """Número al estilo chileno: punto de miles y coma decimal (12.345,6)."""
    texto = f"{valor:,.{decimales}f}"
    return texto.replace(",", "_").replace(".", ",").replace("_", ".")

def _fmt_porcentaje(valor, signo=False):
    return (f"{valor:+.1f}" if signo else f"{valor:.1f}").replace(".", ",") + "%"

def _fmt_compacto(valor):
    """Marcas de eje: 850, 1,5 mil, 12 mil, 1,5 mill. (no "M": en Chile se lee como miles)."""
    absoluto = abs(valor)
    if absoluto >= 1_000_000:
        texto = _fmt_numero(valor / 1_000_000, 1) + " mill."
    elif absoluto >= 10_000:
        texto = _fmt_numero(valor / 1_000) + " mil"
    elif absoluto >= 1_000:
        texto = _fmt_numero(valor / 1_000, 1) + " mil"
    else:
        return _fmt_numero(valor)
    return texto.replace(",0 ", " ")

def _fecha_texto(fecha):
    """'2026-10-26' o un date -> '26-10-2026'."""
    if hasattr(fecha, "strftime"):
        return fecha.strftime("%d-%m-%Y")
    try:
        return datetime.strptime(str(fecha), "%Y-%m-%d").strftime("%d-%m-%Y")
    except ValueError:
        return str(fecha)

_PALABRAS_CATEGORIA = {"semana", "sem", "temporada", "año", "anio", "mes", "dia", "día", "csg",
                       "codigo", "código"}
_PREFIJOS_PORCENTAJE = ("pct", "porc", "desv", "avance", "cumpl", "variac")
_PREFIJOS_CON_SIGNO = ("desv", "variac", "dif")

def _palabras(texto):
    return re.findall(r"[a-záéíóúñ]+", str(texto).lower())

def _es_numero(valor):
    return isinstance(valor, (int, float)) and not isinstance(valor, bool)

_PREFIJOS_RAZON = ("prom", "precio", "tarifa", "ratio", "rendimiento")

def _es_razon(encabezado):
    """Columnas por unidad (USD/caja eq, kg por bin, precio, promedio): no se suman en el
    TOTAL, porque la suma de promedios no significa nada."""
    texto = str(encabezado).lower()
    palabras = _palabras(texto)
    return ("/" in texto or "_caja" in texto or "por" in palabras
            or any(p.startswith(_PREFIJOS_RAZON) for p in palabras))

def _tipo_columna(encabezado, valores):
    """texto | numero | porcentaje | razon (por unidad: USD/caja, promedios; no se suma) |
    categoria (números que no se suman: semana, temporada, CSG) | fecha. Decide cómo se
    muestra, si entra al TOTAL y si puede ir al gráfico."""
    presentes = [v for v in valores if v not in ("", None)]
    if presentes and all(isinstance(v, (date, datetime)) for v in presentes):
        return "fecha"
    if not presentes or not all(_es_numero(v) for v in presentes):
        return "texto"
    palabras = _palabras(encabezado)
    if any(p in _PALABRAS_CATEGORIA for p in palabras):
        return "categoria"
    if "%" in str(encabezado) or any(p.startswith(_PREFIJOS_PORCENTAJE) for p in palabras):
        return "porcentaje"
    if _es_razon(encabezado):
        return "razon"
    return "numero"

def _tipos_columnas(encabezados, filas):
    return [_tipo_columna(h, [f[i] for f in filas]) for i, h in enumerate(encabezados)]

def _lleva_signo(encabezado):
    return any(p.startswith(_PREFIJOS_CON_SIGNO) for p in _palabras(encabezado))

def _decimales_columna(valores):
    """Sin decimales para kilos y cantidades grandes; uno para valores chicos con fracción
    (promedios, kg por bin)."""
    numeros = [abs(v) for v in valores if _es_numero(v)]
    if numeros and max(numeros) < 100 and any(v != int(v) for v in numeros):
        return 1
    return 0

def _texto_celda(valor, tipo, decimales=0, signo=False):
    if valor is None or valor == "":
        return ""
    if tipo == "fecha" and hasattr(valor, "strftime"):
        return valor.strftime("%d-%m-%Y")
    if _es_numero(valor):
        if tipo == "porcentaje":
            return _fmt_porcentaje(valor, signo)
        if tipo == "categoria":
            return str(int(valor)) if valor == int(valor) else str(valor)
        return _fmt_numero(valor, decimales)
    return str(valor)

def _texto_kpi(kpi):
    valor = kpi.get("valor")
    if not _es_numero(valor):
        return str(valor if valor is not None else "-")
    if kpi.get("tipo") == "porcentaje":
        return _fmt_porcentaje(valor, kpi.get("signo", False))
    texto = _fmt_numero(valor, kpi.get("decimales", _decimales_columna([valor])))
    if kpi.get("signo") and valor > 0:
        texto = "+" + texto
    unidad = kpi.get("unidad")
    return f"{texto} {unidad}" if unidad else texto

def _valor_normalizado(valor):
    """Deja un valor de la base listo para el informe: Decimal a float, fechas sin hora a
    date y vacíos como ""."""
    if valor is None:
        return ""
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, datetime) and valor.time() == datetime.min.time():
        return valor.date()
    if isinstance(valor, str):
        return valor.strip()
    return valor

def fila_total(encabezados, filas, tipos, etiqueta="TOTAL"):
    """Suma de las columnas numéricas (no de porcentajes ni de semanas), con la etiqueta
    TOTAL en la primera columna que no es numérica. None si no hay nada que sumar."""
    if len(filas) < 2 or "numero" not in tipos:
        return None
    total = [
        sum(f[i] for f in filas if _es_numero(f[i])) if tipo == "numero" else ""
        for i, tipo in enumerate(tipos)
    ]
    primera = next((i for i, t in enumerate(tipos) if t != "numero"), None)
    if primera is not None:
        total[primera] = etiqueta
    return total

def _buscar_columna(encabezados, nombre):
    """Índice de la columna que calza con `nombre` (exacta, sin tildes ni mayúsculas, o
    contenida en el encabezado)."""
    if not nombre:
        return None
    buscado = _normalizar_nombre(nombre)
    normalizados = [_normalizar_nombre(h) for h in encabezados]
    if buscado in normalizados:
        return normalizados.index(buscado)
    return next((i for i, h in enumerate(normalizados) if buscado in h), None)

def _es_cronologica(encabezado, tipo):
    return tipo in ("fecha", "categoria") or bool(
        set(_palabras(encabezado)) & {"semana", "fecha", "mes", "dia", "día"}
    )

def _unidad_columna(encabezado):
    """Unidad que se deduce del nombre de la columna, para no mezclar unidades en un gráfico."""
    texto = str(encabezado).lower()
    if "usd" in texto or "fob" in texto:
        return "usd"
    if "kg" in texto or "kilo" in texto:
        return "kg"
    if any(p in texto for p in ("caja", "cases", "bulto", "bins", "pallet")):
        return "cajas"
    return None

def grafico_desde_tabla(encabezados, filas, tipos, por=None, valores=None, tipo=None, titulo=None):
    """Gráfico de totales a partir de una tabla: suma las columnas numéricas por la
    categoría elegida (la primera columna de texto si no se indica). Las categorías que no
    son tiempo van de mayor a menor y las que sobran se juntan en "Otros"."""
    if not filas:
        return None
    idx_cat = _buscar_columna(encabezados, por) if por else None
    if idx_cat is None:
        idx_cat = next((i for i, t in enumerate(tipos) if t in ("texto", "categoria", "fecha")), None)
    if idx_cat is None:
        return None
    if valores:
        idx_val = [i for i in (_buscar_columna(encabezados, v) for v in valores)
                   if i is not None and i != idx_cat and tipos[i] in ("numero", "porcentaje", "razon")]
    else:
        numericas = [i for i, t in enumerate(tipos) if t == "numero" and i != idx_cat]
        # La diferencia y el total de la fila repiten información de las otras columnas.
        preferidas = [i for i in numericas
                      if not set(_palabras(encabezados[i])) & {"diferencia", "dif", "total", "saldo"}]
        candidatas = preferidas or numericas
        # Un solo eje: por defecto van juntas solo columnas de la misma unidad que la primera
        # (kilos con kilos, USD con USD); mezclar cajas y dólares en un eje engaña.
        unidad = _unidad_columna(encabezados[candidatas[0]]) if candidatas else None
        idx_val = ([i for i in candidatas if _unidad_columna(encabezados[i]) == unidad][:3]
                   if unidad else candidatas[:1])
    if not idx_val:
        return None

    orden, acumulado = [], {}
    for fila in filas:
        clave = _texto_celda(fila[idx_cat], tipos[idx_cat]) or "(sin dato)"
        if clave not in acumulado:
            acumulado[clave] = [0.0] * len(idx_val)
            orden.append(clave)
        for j, i in enumerate(idx_val):
            if _es_numero(fila[i]):
                acumulado[clave][j] += fila[i]

    tipo = tipo if tipo in TIPOS_GRAFICO else None
    cronologica = _es_cronologica(encabezados[idx_cat], tipos[idx_cat])
    if not cronologica:
        maximo = 6 if tipo == "torta" else MAX_CATEGORIAS_GRAFICO
        orden.sort(key=lambda c: acumulado[c][0], reverse=True)
        if len(orden) > maximo:
            resto = orden[maximo - 1:]
            acumulado["Otros"] = [sum(acumulado[c][j] for c in resto) for j in range(len(idx_val))]
            orden = orden[:maximo - 1] + ["Otros"]
    if tipo is None:
        tipo = ("lineas" if len(orden) > 12 else "barras") if cronologica else "barras_horizontales"
    if tipo == "torta":
        idx_val = idx_val[:1]
    series = [(str(encabezados[i]), [acumulado[c][j] for c in orden]) for j, i in enumerate(idx_val)]
    nombres = _enumerar(nombre for nombre, _ in series)
    return {
        "tipo": tipo,
        "titulo": titulo or f"{nombres} por {str(encabezados[idx_cat]).lower()}",
        "categorias": orden,
        "series": series,
    }

def grafico_totales_por_columna(encabezados, filas, tipos, titulo=None, tipo=None):
    """Gráfico con el total de cada columna numérica (ej. kilos por semana cuando las
    semanas van como columnas). Deja fuera la columna Total."""
    idx = [i for i, t in enumerate(tipos)
           if t == "numero" and _normalizar_nombre(encabezados[i]) != "TOTAL"]
    if len(idx) < 2:
        return None
    return {
        "tipo": tipo if tipo in TIPOS_GRAFICO else ("lineas" if len(idx) > 12 else "barras"),
        "titulo": titulo or "Totales por columna",
        "categorias": [str(encabezados[i]) for i in idx],
        "series": [("Total", [sum(f[i] for f in filas if _es_numero(f[i])) for i in idx])],
    }

def _preparar_informe(informe):
    """Normaliza los valores y deja calculado el tipo de cada columna, para que el PDF y el
    Excel muestren exactamente lo mismo."""
    for seccion in informe.get("secciones", []):
        seccion["filas"] = [[_valor_normalizado(v) for v in fila] for fila in seccion.get("filas") or []]
        if seccion.get("total"):
            seccion["total"] = [_valor_normalizado(v) for v in seccion["total"]]
        encabezados = [str(h) for h in seccion["encabezados"]]
        seccion["encabezados"] = encabezados
        seccion["tipos"] = _tipos_columnas(encabezados, seccion["filas"])
        seccion["decimales"] = [2 if seccion["tipos"][i] == "razon"
                                else _decimales_columna([f[i] for f in seccion["filas"]])
                                for i in range(len(encabezados))]
        seccion["signos"] = [_lleva_signo(h) for h in encabezados]
        grafico = seccion.get("grafico")
        if grafico and (not grafico.get("categorias") or not grafico.get("series")):
            seccion["grafico"] = None
        elif grafico and grafico.get("tipo") == "torta" and any(
                v < 0 for v in grafico["series"][0][1]):
            grafico["tipo"] = "barras_horizontales"  # una torta no puede tener valores negativos
    return informe

def _colores_grafico(grafico):
    """Un color por serie, o por porción en la torta; "Otros" siempre en gris."""
    if grafico["tipo"] == "torta":
        return [COLOR_OTROS if c == "Otros" else COLORES_SERIES[i % len(COLORES_SERIES)]
                for i, c in enumerate(grafico["categorias"])]
    return [COLORES_SERIES[i % len(COLORES_SERIES)] for i in range(len(grafico["series"]))]

# ---------------------------------------------------------------------------- PDF

def _color(hexa):
    from reportlab.lib import colors
    return colors.HexColor(hexa)

def _medidas_leyenda(nombres, ancho_max):
    """Ancho de cada elemento de la leyenda y cuántas líneas ocupa."""
    from reportlab.pdfbase.pdfmetrics import stringWidth
    anchos = [10 + stringWidth(_truncar(str(n), 42), "Helvetica", 7) + 12 for n in nombres]
    lineas, x = 1, 0
    for w in anchos:
        if x + w > ancho_max and x > 0:
            lineas, x = lineas + 1, 0
        x += w
    return anchos, lineas

def _dibujar_leyenda(dibujo, items, x, y_superior, ancho_max):
    from reportlab.graphics.shapes import Rect, String
    anchos, _ = _medidas_leyenda([n for n, _ in items], ancho_max)
    cx, cy = x, y_superior - 8
    for (nombre, color), w in zip(items, anchos):
        if cx + w > x + ancho_max and cx > x:
            cx, cy = x, cy - 11
        dibujo.add(Rect(cx, cy, 7, 7, fillColor=_color(color), strokeColor=None, strokeWidth=0))
        dibujo.add(String(cx + 10, cy + 0.5, _truncar(str(nombre), 42), fontName="Helvetica",
                          fontSize=7, fillColor=_color(COLOR_TINTA_2)))
        cx += w

def _estilo_eje_valores(eje, valores):
    numeros = [v for v in valores if _es_numero(v)] or [0]
    eje.valueMin = min(0, min(numeros))
    if max(numeros) <= eje.valueMin:
        eje.valueMax = eje.valueMin + 1
    eje.labelTextFormat = _fmt_compacto
    eje.labels.fontName = "Helvetica"
    eje.labels.fontSize = 6.5
    eje.labels.fillColor = _color(COLOR_TINTA_SUAVE)
    eje.visibleAxis = 0
    eje.visibleTicks = 0
    eje.visibleGrid = 1
    eje.gridStrokeColor = _color(COLOR_GRILLA)
    eje.gridStrokeWidth = 0.5
    eje.maximumTicks = 6

def _estilo_eje_categorias(eje, etiquetas):
    eje.categoryNames = etiquetas
    eje.strokeColor = _color(COLOR_EJE)
    eje.strokeWidth = 0.6
    eje.visibleTicks = 0
    eje.labels.fontName = "Helvetica"
    eje.labels.fontSize = 6.5
    eje.labels.fillColor = _color(COLOR_TINTA_2)

def _etiquetas_espaciadas(categorias, maximo=24, largo=18):
    """Con muchas categorías (ej. días) se rotula una de cada N para que no se encimen."""
    paso = max(1, -(-len(categorias) // maximo))
    return [_truncar(str(c), largo) if i % paso == 0 else "" for i, c in enumerate(categorias)]

def _dibujo_columnas_o_lineas(grafico, ancho, colores, lineas=False):
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.charts.linecharts import HorizontalLineChart
    from reportlab.graphics.widgets.markers import makeMarker
    categorias, series = grafico["categorias"], grafico["series"]
    n_cat, n_ser = len(categorias), len(series)
    etiquetas = _etiquetas_espaciadas(categorias)
    girar = sum(len(e) for e in etiquetas) * 4.2 > ancho - 50
    alto_leyenda = 0
    if n_ser > 1:
        alto_leyenda = 11 * _medidas_leyenda([n for n, _ in series], ancho - 50)[1] + 4
    margen_inferior = (38 if girar else 16) + alto_leyenda
    alto = 130 + margen_inferior
    dibujo = Drawing(ancho, alto)
    grafico_rl = HorizontalLineChart() if lineas else VerticalBarChart()
    grafico_rl.x, grafico_rl.y = 44, margen_inferior
    grafico_rl.width, grafico_rl.height = ancho - 50, alto - margen_inferior - 8
    grafico_rl.data = [list(valores) for _, valores in series]
    _estilo_eje_valores(grafico_rl.valueAxis, [v for _, vals in series for v in vals])
    _estilo_eje_categorias(grafico_rl.categoryAxis, etiquetas)
    if girar:
        grafico_rl.categoryAxis.labels.angle = 35
        grafico_rl.categoryAxis.labels.boxAnchor = "ne"
        grafico_rl.categoryAxis.labels.dy = -2
    else:
        grafico_rl.categoryAxis.labels.boxAnchor = "n"
        grafico_rl.categoryAxis.labels.dy = -3
    if lineas:
        grafico_rl.joinedLines = 1
        for i, color in enumerate(colores):
            grafico_rl.lines[i].strokeColor = _color(color)
            grafico_rl.lines[i].strokeWidth = 1.6
            if n_cat <= 24:
                marcador = makeMarker("FilledCircle")
                marcador.size = 4.5
                marcador.fillColor = _color(color)
                marcador.strokeColor = _color("#ffffff")
                marcador.strokeWidth = 1
                grafico_rl.lines[i].symbol = marcador
    else:
        # Barras finas (máx. ~16 pt) con un pequeño espacio entre las de un mismo grupo.
        ranura = grafico_rl.width / max(n_cat, 1)
        barra = min(16, ranura * 0.72 / n_ser)
        grafico_rl.barWidth = barra
        grafico_rl.barSpacing = 1.5 if n_ser > 1 else 0
        grafico_rl.groupSpacing = max(ranura - barra * n_ser - grafico_rl.barSpacing * (n_ser - 1), 2)
        for i, color in enumerate(colores):
            grafico_rl.bars[i].fillColor = _color(color)
            grafico_rl.bars[i].strokeColor = None
            grafico_rl.bars[i].strokeWidth = 0
        if n_ser == 1 and "Otros" in categorias:
            grafico_rl.bars[(0, categorias.index("Otros"))].fillColor = _color(COLOR_OTROS)
        if n_ser == 1 and n_cat <= 8:
            # Pocas columnas: el valor va sobre cada una (en papel no hay tooltips).
            grafico_rl.barLabelFormat = _fmt_compacto
            grafico_rl.barLabels.fontName = "Helvetica"
            grafico_rl.barLabels.fontSize = 6.5
            grafico_rl.barLabels.fillColor = _color(COLOR_TINTA_2)
            grafico_rl.barLabels.boxAnchor = "s"
            grafico_rl.barLabels.dy = 3
    dibujo.add(grafico_rl)
    if n_ser > 1:
        _dibujar_leyenda(dibujo, [(n, c) for (n, _), c in zip(series, colores)],
                         grafico_rl.x, alto_leyenda, ancho - 50)
    return dibujo

def _dibujo_barras_horizontales(grafico, ancho, colores):
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.barcharts import HorizontalBarChart
    from reportlab.pdfbase.pdfmetrics import stringWidth
    categorias, series = grafico["categorias"], grafico["series"]
    n_cat, n_ser = len(categorias), len(series)
    etiquetas = [_truncar(str(c), 36) for c in categorias]
    ancho_etiquetas = min(max(stringWidth(e, "Helvetica", 6.5) for e in etiquetas) + 8, ancho * 0.42)
    barra = 9 if n_ser == 1 else 6.5
    alto_fila = barra * n_ser + 1.5 * (n_ser - 1) + 6
    alto_leyenda = 0
    if n_ser > 1:
        alto_leyenda = 11 * _medidas_leyenda([n for n, _ in series], ancho - ancho_etiquetas)[1] + 4
    margen_inferior = 16 + alto_leyenda
    alto = alto_fila * n_cat + margen_inferior + 6
    dibujo = Drawing(ancho, alto)
    hb = HorizontalBarChart()
    hb.x, hb.y = ancho_etiquetas, margen_inferior
    hb.width, hb.height = ancho - ancho_etiquetas - 40, alto_fila * n_cat
    # Dentro de cada grupo reportlab dibuja la primera serie abajo: se invierten para que
    # se lean de arriba hacia abajo en el mismo orden que la leyenda (estimado, luego real).
    hb.data = [list(valores) for _, valores in series][::-1]
    hb.barWidth = barra
    hb.barSpacing = 1.5 if n_ser > 1 else 0
    hb.groupSpacing = 6
    _estilo_eje_valores(hb.valueAxis, [v for _, vals in series for v in vals])
    _estilo_eje_categorias(hb.categoryAxis, etiquetas)
    hb.categoryAxis.reverseDirection = 1  # la categoría más grande arriba
    hb.categoryAxis.labels.boxAnchor = "e"
    hb.categoryAxis.labels.dx = -4
    for i, color in enumerate(colores[::-1]):
        hb.bars[i].fillColor = _color(color)
        hb.bars[i].strokeColor = None
        hb.bars[i].strokeWidth = 0
    if n_ser == 1:
        if "Otros" in categorias:
            hb.bars[(0, categorias.index("Otros"))].fillColor = _color(COLOR_OTROS)
        # Con una sola serie el valor va en la punta de cada barra (el PDF no tiene
        # tooltips); con varias se lee en la tabla.
        hb.barLabelFormat = _fmt_compacto
        hb.barLabels.fontName = "Helvetica"
        hb.barLabels.fontSize = 6
        hb.barLabels.fillColor = _color(COLOR_TINTA_2)
        hb.barLabels.boxAnchor = "w"
        hb.barLabels.dx = 3
    dibujo.add(hb)
    if n_ser > 1:
        _dibujar_leyenda(dibujo, [(n, c) for (n, _), c in zip(series, colores)],
                         hb.x, alto_leyenda, ancho - ancho_etiquetas)
    return dibujo

def _dibujo_torta(grafico, ancho, colores):
    from reportlab.graphics.shapes import Drawing, Rect, String
    from reportlab.graphics.charts.piecharts import Pie
    categorias, valores = grafico["categorias"], grafico["series"][0][1]
    total = sum(v for v in valores if _es_numero(v)) or 1
    alto = max(150, 14 * len(categorias) + 20)
    dibujo = Drawing(ancho, alto)
    torta = Pie()
    diametro = 128
    torta.x, torta.y = 12, (alto - diametro) / 2
    torta.width = torta.height = diametro
    torta.data = list(valores)
    torta.labels = None
    torta.slices.strokeColor = _color("#ffffff")  # separación entre porciones
    torta.slices.strokeWidth = 1.5
    for i, color in enumerate(colores):
        torta.slices[i].fillColor = _color(color)
    dibujo.add(torta)
    y = alto - 22
    for categoria, valor, color in zip(categorias, valores, colores):
        dibujo.add(Rect(170, y, 7, 7, fillColor=_color(color), strokeColor=None, strokeWidth=0))
        texto = f"{_truncar(str(categoria), 40)}  {_fmt_porcentaje(valor / total * 100)}  ({_fmt_compacto(valor)})"
        dibujo.add(String(181, y + 0.5, texto, fontName="Helvetica", fontSize=7,
                          fillColor=_color(COLOR_TINTA_2)))
        y -= 14
    return dibujo

def _dibujo_grafico(grafico, ancho):
    colores = _colores_grafico(grafico)
    if grafico["tipo"] == "torta":
        return _dibujo_torta(grafico, ancho, colores)
    if grafico["tipo"] == "lineas":
        return _dibujo_columnas_o_lineas(grafico, ancho, colores, lineas=True)
    if grafico["tipo"] == "barras":
        return _dibujo_columnas_o_lineas(grafico, ancho, colores)
    return _dibujo_barras_horizontales(grafico, ancho, colores)

def _tamano_letra_tabla(n_columnas):
    return 8 if n_columnas <= 7 else 7 if n_columnas <= 11 else 6.2 if n_columnas <= 16 else 5.4

def _textos_seccion(seccion):
    tipos, decimales, signos = seccion["tipos"], seccion["decimales"], seccion["signos"]
    def fila_texto(fila):
        return [_texto_celda(v, tipos[i], decimales[i], signos[i]) for i, v in enumerate(fila)]
    textos = [fila_texto(f) for f in seccion["filas"]]
    total = fila_texto(seccion["total"]) if seccion.get("total") else None
    return textos, total

def _anchos_columnas(encabezados, textos, tipos, tamano, disponible):
    """Ancho de cada columna según su contenido. Si no caben, se angostan primero las de
    texto (que pueden partirse en dos líneas) y al final se escala todo."""
    from reportlab.pdfbase.pdfmetrics import stringWidth
    muestra = textos[:400]
    anchos = []
    for i, encabezado in enumerate(encabezados):
        datos = max([stringWidth(f[i], "Helvetica", tamano) for f in muestra] or [0])
        palabras = str(encabezado).split() or [""]
        titulo = max(stringWidth(p, "Helvetica-Bold", tamano) for p in palabras)
        ancho = max(datos, titulo) + 8
        if tipos[i] == "texto":
            ancho = min(ancho, 170)
        anchos.append(max(ancho, 22))
    total = sum(anchos)
    if total > disponible:
        textos_idx = [i for i, t in enumerate(tipos) if t == "texto"]
        reducible = sum(max(anchos[i] - 55, 0) for i in textos_idx)
        if reducible:
            factor = min(1, (total - disponible) / reducible)
            for i in textos_idx:
                anchos[i] -= max(anchos[i] - 55, 0) * factor
        total = sum(anchos)
        if total > disponible:
            anchos = [a * disponible / total for a in anchos]
    return anchos

def _ancho_natural(seccion):
    textos, total = _textos_seccion(seccion)
    tamano = _tamano_letra_tabla(len(seccion["encabezados"]))
    return sum(_anchos_columnas(seccion["encabezados"], textos + ([total] if total else []),
                                seccion["tipos"], tamano, 10 ** 6))

def _tabla_pdf(seccion, disponible):
    from reportlab.lib.enums import TA_RIGHT
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Table, TableStyle, Paragraph
    from reportlab.pdfbase.pdfmetrics import stringWidth
    from xml.sax.saxutils import escape
    encabezados, tipos = seccion["encabezados"], seccion["tipos"]
    textos, total = _textos_seccion(seccion)
    tamano = _tamano_letra_tabla(len(encabezados))
    anchos = _anchos_columnas(encabezados, textos + ([total] if total else []), tipos, tamano, disponible)
    numericas = {i for i, t in enumerate(tipos) if t in ("numero", "porcentaje", "razon")}
    estilo_celda = ParagraphStyle("celda", fontName="Helvetica", fontSize=tamano,
                                  leading=tamano + 1.6, textColor=_color(COLOR_TINTA))
    estilo_total = ParagraphStyle("celda_total", parent=estilo_celda, fontName="Helvetica-Bold")
    estilo_enc = ParagraphStyle("enc", parent=estilo_celda, fontName="Helvetica-Bold",
                                textColor=_color("#ffffff"))
    estilo_enc_num = ParagraphStyle("enc_num", parent=estilo_enc, alignment=TA_RIGHT)

    def celda(texto, i, estilo):
        # Solo el texto que no cabe en una línea va como Paragraph (que lo parte); el resto
        # va plano, que es mucho más rápido en tablas de miles de filas.
        if i in numericas or stringWidth(texto, estilo.fontName, tamano) <= anchos[i] - 6:
            return texto
        return Paragraph(escape(texto), estilo)

    datos = [[Paragraph(escape(h), estilo_enc_num if i in numericas else estilo_enc)
              for i, h in enumerate(encabezados)]]
    datos += [[celda(t, i, estilo_celda) for i, t in enumerate(fila)] for fila in textos]
    if total:
        datos.append([celda(t, i, estilo_total) for i, t in enumerate(total)])
    ultima_dato = len(textos)
    estilo = [
        ("BACKGROUND", (0, 0), (-1, 0), _color(COLOR_MARCA)),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), tamano),
        ("TEXTCOLOR", (0, 1), (-1, -1), _color(COLOR_TINTA)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.2),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("ROWBACKGROUNDS", (0, 1), (-1, ultima_dato), [_color("#ffffff"), _color(COLOR_FONDO_FILA)]),
        ("LINEBELOW", (0, 1), (-1, ultima_dato), 0.25, _color(COLOR_GRILLA)),
    ]
    for i in numericas:
        estilo.append(("ALIGN", (i, 1), (i, -1), "RIGHT"))
    if total:
        estilo += [
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("BACKGROUND", (0, -1), (-1, -1), _color(COLOR_FONDO_TOTAL)),
            ("LINEABOVE", (0, -1), (-1, -1), 0.8, _color(COLOR_MARCA)),
        ]
    tabla = Table(datos, colWidths=anchos, repeatRows=1, hAlign="LEFT")
    tabla.setStyle(TableStyle(estilo))
    return tabla

def _parrafos_resumen(texto, estilo, estilo_vineta):
    """El resumen viene escrito como en WhatsApp: *negrita*, _cursiva_ y viñetas con "-"."""
    from reportlab.platypus import Paragraph
    from xml.sax.saxutils import escape
    parrafos = []
    for linea in str(texto).splitlines():
        linea = linea.strip()
        if not linea:
            continue
        vineta = linea[:2] in ("- ", "• ", "* ")
        if vineta:
            linea = linea[2:]
        html = escape(linea)
        html = re.sub(r"\*([^*]+)\*", r"<b>\1</b>", html)
        html = re.sub(r"(?<!\w)_([^_]+)_(?!\w)", r"<i>\1</i>", html)
        parrafos.append(Paragraph(f"•&nbsp;&nbsp;{html}" if vineta else html,
                                  estilo_vineta if vineta else estilo))
    return parrafos

def _tabla_kpis(kpis, ancho):
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Table, TableStyle, Paragraph
    from xml.sax.saxutils import escape
    por_fila = min(len(kpis), 5)
    separacion = 6
    ancho_ficha = ancho / por_fila - separacion
    estilo = ParagraphStyle("kpi", fontName="Helvetica", fontSize=7, leading=16,
                            textColor=_color(COLOR_TINTA))

    def ficha(kpi):
        contenido = Paragraph(
            f'<font size="7" color="{COLOR_TINTA_SUAVE}">{escape(str(kpi["etiqueta"]))}</font><br/>'
            f'<font size="13"><b>{escape(_texto_kpi(kpi))}</b></font>', estilo)
        tarjeta = Table([[contenido]], colWidths=[ancho_ficha])
        tarjeta.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), _color(COLOR_FONDO_KPI)),
            ("LINEBEFORE", (0, 0), (0, -1), 2.5, _color(COLOR_MARCA)),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ]))
        return tarjeta

    fichas = [ficha(k) for k in kpis]
    filas = [fichas[i:i + por_fila] for i in range(0, len(fichas), por_fila)]
    filas[-1] += [""] * (por_fila - len(filas[-1]))
    tabla = Table(filas, colWidths=[ancho / por_fila] * por_fila, hAlign="LEFT")
    tabla.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), separacion),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), separacion),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return tabla

def generar_pdf_informe(informe):
    """PDF con formato de informe: franja de encabezado, título y periodo, cifras clave,
    resumen, gráfico, tabla (con encabezado repetido en cada página) y notas al pie."""
    from reportlab.lib.pagesizes import LETTER, ELEVENSEVENTEEN, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate, Paragraph, Spacer, KeepTogether
    from xml.sax.saxutils import escape

    margen = 14 * mm
    secciones = informe["secciones"]
    ancho_necesario = max([_ancho_natural(s) for s in secciones if s["filas"]] or [0])
    # Vertical si la tabla cabe; si no, carta horizontal; y para tablas muy anchas (ej. 12
    # semanas con 3 fuentes) una hoja 11x17 horizontal, en vez de letras ilegibles.
    if ancho_necesario <= LETTER[0] - 2 * margen:
        tamano_pagina = LETTER
    elif ancho_necesario <= (LETTER[1] - 2 * margen) * 1.15:
        tamano_pagina = landscape(LETTER)
    else:
        tamano_pagina = landscape(ELEVENSEVENTEEN)

    buffer = io.BytesIO()
    doc = BaseDocTemplate(
        buffer, pagesize=tamano_pagina, leftMargin=margen, rightMargin=margen,
        topMargin=19 * mm, bottomMargin=15 * mm, title=informe["titulo"],
        author="Asistente de Cosecha Agua Santa",
    )
    generado = datetime.now().strftime("%d-%m-%Y %H:%M")

    def decorar_pagina(canvas, documento):
        ancho_pag, alto_pag = documento.pagesize
        canvas.saveState()
        canvas.setFillColor(_color(COLOR_MARCA))
        canvas.rect(0, alto_pag - 11 * mm, ancho_pag, 11 * mm, stroke=0, fill=1)
        canvas.setFillColor(_color("#ffffff"))
        canvas.setFont("Helvetica-Bold", 9)
        canvas.drawString(margen, alto_pag - 7 * mm, "AGUA SANTA  ·  Asistente de Cosecha")
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(ancho_pag - margen, alto_pag - 7 * mm, f"Generado el {generado}")
        canvas.setFillColor(_color(COLOR_TINTA_SUAVE))
        canvas.setFont("Helvetica", 6.5)
        canvas.drawString(margen, 8 * mm, _truncar(informe["titulo"], 120))
        canvas.drawRightString(ancho_pag - margen, 8 * mm, f"Página {documento.page}")
        canvas.restoreState()

    marco = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="cuerpo",
                  leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
    doc.addPageTemplates([PageTemplate(id="informe", frames=[marco], onPage=decorar_pagina)])

    est_titulo = ParagraphStyle("titulo", fontName="Helvetica-Bold", fontSize=16, leading=20,
                                textColor=_color(COLOR_TINTA))
    est_subtitulo = ParagraphStyle("subtitulo", fontName="Helvetica", fontSize=9, leading=12,
                                   textColor=_color(COLOR_TINTA_2))
    est_h2 = ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=11, leading=14,
                            textColor=_color(COLOR_MARCA), spaceAfter=4)
    est_grafico = ParagraphStyle("grafico", fontName="Helvetica-Bold", fontSize=8.5, leading=11,
                                 textColor=_color(COLOR_TINTA_2), spaceAfter=3)
    est_texto = ParagraphStyle("texto", fontName="Helvetica", fontSize=9, leading=12.5,
                               textColor=_color(COLOR_TINTA), spaceAfter=3)
    est_vineta = ParagraphStyle("vineta", parent=est_texto, leftIndent=10, firstLineIndent=-7)
    est_nota = ParagraphStyle("nota", fontName="Helvetica-Oblique", fontSize=7, leading=9.5,
                              textColor=_color(COLOR_TINTA_SUAVE), spaceBefore=3)

    historia = [Paragraph(escape(informe["titulo"]), est_titulo)]
    if informe.get("subtitulo"):
        historia.append(Paragraph(escape(informe["subtitulo"]), est_subtitulo))
    historia.append(Spacer(1, 5 * mm))
    if informe.get("kpis"):
        historia += [_tabla_kpis(informe["kpis"], doc.width), Spacer(1, 5 * mm)]
    if informe.get("resumen"):
        historia.append(Paragraph("Resumen", est_h2))
        historia += _parrafos_resumen(informe["resumen"], est_texto, est_vineta)
        historia.append(Spacer(1, 4 * mm))
    ancho_grafico = min(doc.width, 520)
    for seccion in secciones:
        bloque = []
        if seccion.get("titulo") and len(secciones) > 1:
            bloque.append(Paragraph(escape(seccion["titulo"]), est_h2))
        if seccion.get("grafico"):
            bloque += [Paragraph(escape(seccion["grafico"]["titulo"]), est_grafico),
                       _dibujo_grafico(seccion["grafico"], ancho_grafico), Spacer(1, 4 * mm)]
        if bloque:
            historia.append(KeepTogether(bloque))
        if seccion["filas"]:
            historia.append(_tabla_pdf(seccion, doc.width))
        if seccion.get("nota"):
            historia.append(Paragraph(escape(seccion["nota"]), est_nota))
        historia.append(Spacer(1, 6 * mm))
    for nota in informe.get("notas") or []:
        historia.append(Paragraph(escape(nota), est_nota))
    doc.build(historia)
    return buffer.getvalue()

# ---------------------------------------------------------------------------- Excel

def _formato_excel(tipo, decimales, signo):
    if tipo == "porcentaje":
        return '+0.0"%";-0.0"%";0.0"%"' if signo else '0.0"%"'
    if tipo == "categoria":
        return "0"
    if tipo == "fecha":
        return "dd-mm-yyyy"
    if decimales:
        return "#,##0." + "0" * decimales
    return "#,##0"

def _grafico_excel(hoja_datos, fila_inicio, grafico):
    """Gráfico nativo de Excel. Sus datos quedan en la hoja "Datos gráfico" (Excel necesita
    leerlos de celdas). Devuelve (gráfico, alto en cm, próxima fila libre en esa hoja)."""
    from openpyxl.chart import BarChart, LineChart, PieChart, Reference
    from openpyxl.chart.axis import ChartLines
    from openpyxl.chart.series import DataPoint
    from openpyxl.chart.shapes import GraphicalProperties
    from openpyxl.drawing.line import LineProperties
    from openpyxl.styles import Font

    categorias, series = grafico["categorias"], grafico["series"]
    hoja_datos.cell(fila_inicio, 1, grafico["titulo"]).font = Font(bold=True)
    encabezado = fila_inicio + 1
    hoja_datos.cell(encabezado, 1, "Categoría").font = Font(bold=True)
    for j, (nombre, valores) in enumerate(series):
        hoja_datos.cell(encabezado, 2 + j, nombre).font = Font(bold=True)
        for i, valor in enumerate(valores):
            celda = hoja_datos.cell(encabezado + 1 + i, 2 + j, round(valor, 2) if _es_numero(valor) else 0)
            celda.number_format = "#,##0"
    for i, categoria in enumerate(categorias):
        hoja_datos.cell(encabezado + 1 + i, 1, str(categoria))

    tipo = grafico["tipo"]
    colores = _colores_grafico(grafico)
    if tipo == "torta":
        grafico_xl = PieChart()
    elif tipo == "lineas":
        grafico_xl = LineChart()
    else:
        grafico_xl = BarChart()
        grafico_xl.type = "bar" if tipo == "barras_horizontales" else "col"
        grafico_xl.grouping = "clustered"
        grafico_xl.gapWidth = 70
        grafico_xl.overlap = -8 if len(series) > 1 else 0
    datos = Reference(hoja_datos, min_col=2, max_col=1 + len(series),
                      min_row=encabezado, max_row=encabezado + len(categorias))
    etiquetas = Reference(hoja_datos, min_col=1, min_row=encabezado + 1,
                          max_row=encabezado + len(categorias))
    grafico_xl.add_data(datos, titles_from_data=True)
    grafico_xl.set_categories(etiquetas)

    if tipo == "torta":
        serie = grafico_xl.series[0]
        for i, color in enumerate(colores):
            punto = DataPoint(idx=i)
            punto.graphicalProperties.solidFill = color[1:]
            punto.graphicalProperties.line.solidFill = "FFFFFF"
            serie.dPt.append(punto)
    else:
        for serie, color in zip(grafico_xl.series, colores):
            if tipo == "lineas":
                serie.graphicalProperties.line.solidFill = color[1:]
                serie.graphicalProperties.line.width = 22000
                serie.smooth = False
                serie.marker.symbol = "circle"
                serie.marker.size = 5
                serie.marker.graphicalProperties.solidFill = color[1:]
                serie.marker.graphicalProperties.line.solidFill = color[1:]
            else:
                serie.graphicalProperties.solidFill = color[1:]
                serie.graphicalProperties.line.noFill = True
        if len(series) == 1 and "Otros" in categorias and tipo != "lineas":
            punto = DataPoint(idx=categorias.index("Otros"))
            punto.graphicalProperties.solidFill = COLOR_OTROS[1:]
            punto.graphicalProperties.line.noFill = True
            grafico_xl.series[0].dPt.append(punto)
        grafico_xl.y_axis.number_format = "#,##0"
        grafico_xl.y_axis.majorGridlines = ChartLines(
            spPr=GraphicalProperties(ln=LineProperties(solidFill=COLOR_GRILLA[1:])))
        grafico_xl.x_axis.delete = False
        grafico_xl.y_axis.delete = False
        if tipo == "barras_horizontales":
            # Excel dibuja las barras horizontales de abajo hacia arriba: se invierte para
            # que la primera (la mayor) quede arriba, y el eje de valores vuelve abajo.
            grafico_xl.x_axis.scaling.orientation = "maxMin"
            grafico_xl.y_axis.crosses = "max"
    if len(series) == 1 and tipo != "torta":
        grafico_xl.legend = None
    else:
        grafico_xl.legend.position = "b"
    alto = 7.5
    if tipo == "barras_horizontales":
        alto = max(7.5, 0.55 * len(categorias) * max(1, len(series) * 0.7) + 2.5)
    grafico_xl.height = alto
    grafico_xl.width = 17
    return grafico_xl, alto, encabezado + len(categorias) + 3

def generar_excel_informe(informe):
    """Excel con formato de informe en la hoja "Informe" (cifras clave, resumen, gráfico
    nativo y la tabla con filtros y fila TOTAL). Los números quedan como números, para que
    el EAS pueda filtrar y sumar sin reformatear nada."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter

    def relleno(hexa):
        return PatternFill("solid", start_color=hexa[1:], end_color=hexa[1:])

    libro = Workbook()
    hoja = libro.active
    hoja.title = "Informe"
    hoja.sheet_view.showGridLines = False
    hoja_datos = None
    fila_datos = 1
    secciones = informe["secciones"]
    n_columnas = max([len(s["encabezados"]) for s in secciones] + [6])
    anchos = {}

    def medir(columna, texto, extra=2):
        anchos[columna] = max(anchos.get(columna, 0), min(len(str(texto)) + extra, 48))

    hoja.cell(1, 1, informe["titulo"]).font = Font(bold=True, size=15, color=COLOR_MARCA[1:])
    if informe.get("subtitulo"):
        hoja.cell(2, 1, informe["subtitulo"]).font = Font(size=10, color=COLOR_TINTA_2[1:])
    hoja.cell(3, 1, f"Generado el {datetime.now().strftime('%d-%m-%Y %H:%M')} por el "
                    f"Asistente de Cosecha Agua Santa").font = Font(size=8, italic=True,
                                                                    color=COLOR_TINTA_SUAVE[1:])
    fila = 5

    kpis = informe.get("kpis") or []
    borde_kpi = Border(left=Side(style="thick", color="FFFFFF"), right=Side(style="thick", color="FFFFFF"))
    for inicio in range(0, len(kpis), n_columnas):
        for j, kpi in enumerate(kpis[inicio:inicio + n_columnas]):
            etiqueta = hoja.cell(fila, 1 + j, kpi["etiqueta"])
            etiqueta.font = Font(size=8, color=COLOR_TINTA_SUAVE[1:])
            etiqueta.alignment = Alignment(wrap_text=True, vertical="top")
            valor = hoja.cell(fila + 1, 1 + j, kpi["valor"])
            valor.font = Font(size=13, bold=True, color=COLOR_TINTA[1:])
            if _es_numero(kpi["valor"]):
                tipo_kpi = kpi.get("tipo") if kpi.get("tipo") in ("porcentaje",) else "numero"
                formato = _formato_excel(tipo_kpi, kpi.get("decimales", _decimales_columna([kpi["valor"]])),
                                         kpi.get("signo"))
                if tipo_kpi == "numero":
                    unidad = f'" {kpi["unidad"]}"' if kpi.get("unidad") else ""
                    formato = (f"+{formato}{unidad};-{formato}{unidad};0{unidad}" if kpi.get("signo")
                               else f"{formato}{unidad}")
                valor.number_format = formato
            for celda in (etiqueta, valor):
                celda.fill = relleno(COLOR_FONDO_KPI)
                celda.border = borde_kpi
            medir(1 + j, _texto_kpi(kpi), extra=4)
        fila += 3

    if informe.get("resumen"):
        hoja.cell(fila, 1, "Resumen").font = Font(bold=True, size=11, color=COLOR_MARCA[1:])
        fila += 1
        texto = re.sub(r"\*([^*]+)\*", r"\1", str(informe["resumen"])).strip()
        hoja.merge_cells(start_row=fila, start_column=1, end_row=fila, end_column=max(n_columnas, 8))
        celda = hoja.cell(fila, 1, texto)
        celda.alignment = Alignment(wrap_text=True, vertical="top")
        celda.font = Font(size=10)
        lineas = sum(1 + len(l) // 110 for l in texto.splitlines() or [""])
        hoja.row_dimensions[fila].height = 15 * lineas + 4
        fila += 2

    borde_fila = Border(bottom=Side(style="hair", color=COLOR_GRILLA[1:]))
    borde_total = Border(top=Side(style="medium", color=COLOR_MARCA[1:]))
    hay_filtro = False
    for seccion in secciones:
        if seccion.get("titulo") and len(secciones) > 1:
            hoja.cell(fila, 1, seccion["titulo"]).font = Font(bold=True, size=12, color=COLOR_MARCA[1:])
            fila += 1
        if seccion.get("grafico"):
            if hoja_datos is None:
                hoja_datos = libro.create_sheet("Datos gráfico")
            hoja.cell(fila, 1, seccion["grafico"]["titulo"]).font = Font(bold=True, size=10,
                                                                        color=COLOR_TINTA_2[1:])
            fila += 1
            grafico_xl, alto_cm, fila_datos = _grafico_excel(hoja_datos, fila_datos, seccion["grafico"])
            hoja.add_chart(grafico_xl, f"A{fila}")
            fila += int(alto_cm / 0.53) + 2  # una fila mide ~0,53 cm
        encabezados, tipos = seccion["encabezados"], seccion["tipos"]
        if seccion["filas"]:
            fila_encabezado = fila
            for j, encabezado in enumerate(encabezados):
                celda = hoja.cell(fila, 1 + j, encabezado)
                celda.font = Font(bold=True, color="FFFFFF")
                celda.fill = relleno(COLOR_MARCA)
                celda.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                medir(1 + j, max(str(encabezado).split() or [""], key=len), extra=4)
            if any(len(str(h)) > 14 for h in encabezados):
                hoja.row_dimensions[fila].height = 30
            formatos = [_formato_excel(t, d, s) for t, d, s in
                        zip(tipos, seccion["decimales"], seccion["signos"])]
            textos, texto_total = _textos_seccion(seccion)
            for k, (valores, textos_fila) in enumerate(zip(seccion["filas"], textos)):
                fila += 1
                for j, valor in enumerate(valores):
                    celda = hoja.cell(fila, 1 + j, valor if valor != "" else None)
                    if tipos[j] != "texto":
                        celda.number_format = formatos[j]
                    celda.border = borde_fila
                    if k % 2:
                        celda.fill = relleno(COLOR_FONDO_FILA)
                    medir(1 + j, textos_fila[j])
            if not hay_filtro:
                # Excel admite un solo filtro por hoja: va en la tabla principal.
                hoja.auto_filter.ref = (f"A{fila_encabezado}:"
                                        f"{get_column_letter(len(encabezados))}{fila}")
                hay_filtro = True
            if seccion.get("total"):
                fila += 1
                for j, valor in enumerate(seccion["total"]):
                    celda = hoja.cell(fila, 1 + j, valor if valor != "" else None)
                    celda.font = Font(bold=True)
                    celda.fill = relleno(COLOR_FONDO_TOTAL)
                    celda.border = borde_total
                    if tipos[j] != "texto":
                        celda.number_format = formatos[j]
            fila += 1
        if seccion.get("nota"):
            hoja.cell(fila, 1, seccion["nota"]).font = Font(size=8, italic=True, color=COLOR_TINTA_SUAVE[1:])
            fila += 1
        fila += 2

    for nota in informe.get("notas") or []:
        hoja.cell(fila, 1, nota).font = Font(size=8, italic=True, color=COLOR_TINTA_SUAVE[1:])
        fila += 1
    for columna, ancho in anchos.items():
        hoja.column_dimensions[get_column_letter(columna)].width = max(ancho, 10)
    hoja.page_setup.orientation = "landscape" if n_columnas > 6 else "portrait"
    hoja.page_setup.fitToWidth = 1
    hoja.page_setup.fitToHeight = 0
    hoja.sheet_properties.pageSetUpPr.fitToPage = True
    if hoja_datos is not None:
        hoja_datos.column_dimensions["A"].width = 40
    buffer = io.BytesIO()
    libro.save(buffer)
    return buffer.getvalue()

# ---------------------------------------------------------------------------- Informes del bot

def informe_cosecha_flexible(filas, dimensiones, unidad, base_estimado, fecha_inicio, fecha_fin,
                             filtro_desc="", grafico=None, resumen=None):
    """Informe de estimado vs real con las dimensiones pedidas. Por defecto el gráfico
    muestra el estimado y el real por la primera dimensión (o el total si no hay ninguna)."""
    etiqueta_est = ETIQUETA_FUENTE.get(base_estimado, "Estimado")
    encabezados, filas_tabla = _filas_para_archivo(filas, dimensiones, unidad, base_estimado)
    filas_tabla = [[_valor_normalizado(v) for v in fila] for fila in filas_tabla]
    n = len(dimensiones)
    total_est = sum(f[n] for f in filas_tabla)
    total_real = sum(f[n + 1] for f in filas_tabla)
    desviacion = round((total_real - total_est) / total_est * 100, 1) if total_est else ""
    if n:
        total = ["TOTAL"] + [""] * (n - 1) + [total_est, total_real, total_real - total_est, desviacion]
    else:
        encabezados = ["Periodo"] + encabezados
        filas_tabla = [[f"{_fecha_texto(fecha_inicio)} al {_fecha_texto(fecha_fin)}"] + f
                       for f in filas_tabla]
        total = None
    kpis = [
        {"etiqueta": etiqueta_est, "valor": total_est, "unidad": unidad},
        {"etiqueta": "Real", "valor": total_real, "unidad": unidad},
        {"etiqueta": f"Diferencia real − {etiqueta_est.lower()}", "valor": total_real - total_est,
         "unidad": unidad, "signo": True},
    ]
    if total_est:
        kpis.append({"etiqueta": f"Real vs {etiqueta_est}", "valor": desviacion,
                     "tipo": "porcentaje", "signo": True})

    pedido = grafico or {}
    if pedido.get("tipo") == "ninguno":
        dibujo = None
    elif not n:
        dibujo = {"tipo": "barras", "titulo": pedido.get("titulo") or f"{etiqueta_est} vs real ({unidad})",
                  "categorias": ["Total del periodo"],
                  "series": [(etiqueta_est, [total_est]), ("Real", [total_real])]}
    else:
        por = pedido.get("por")
        dibujo = grafico_desde_tabla(
            encabezados, filas_tabla, _tipos_columnas(encabezados, filas_tabla),
            por=DIMENSIONES_ETIQUETA.get(por, por) if por else None,
            tipo=pedido.get("tipo"), titulo=pedido.get("titulo"),
        )
    por_texto = (" por " + _enumerar(DIMENSIONES_ETIQUETA.get(d, d).lower() for d in dimensiones)
                 if dimensiones else "")
    return {
        "titulo": f"Cosecha: {etiqueta_est} vs real{por_texto}",
        "subtitulo": (f"Del {_fecha_texto(fecha_inicio)} al {_fecha_texto(fecha_fin)}"
                      f"{filtro_desc} · cifras en {unidad}"),
        "resumen": resumen,
        "kpis": kpis,
        "secciones": [{"titulo": "Detalle", "encabezados": encabezados, "filas": filas_tabla,
                       "total": total, "grafico": dibujo}],
        "notas": [NOTA_FUENTES],
    }

def informe_proyeccion(filas, dimensiones, fuentes, encabezados, filas_tabla, inicio, fin,
                       filtros_desc=None, grafico=None, resumen=None):
    """Informe de proyección: las semanas como columnas y, por defecto, un gráfico con los
    kilos de cada semana (una barra por fuente)."""
    n_dim = len(dimensiones)
    filas = [list(f[:n_dim + 2]) + [float(f[n_dim + 2] or 0)] for f in filas]
    datos, total = filas_tabla[:-1], filas_tabla[-1]
    nombres = {FUENTES_PROYECCION[f][0]: FUENTES_PROYECCION[f][1] for f in fuentes}
    lunes, dia = {}, inicio
    while dia <= fin:
        lunes[dia.isocalendar()[1]] = dia
        dia += timedelta(days=7)
    semanas = sorted({f[n_dim] for f in filas}, key=orden_semana)
    rotulo = {s: (f"S{etiqueta_semana(s)} ({lunes[s].strftime('%d-%m')})" if s in lunes
                  else f"S{etiqueta_semana(s)}") for s in semanas}
    por_fuente, por_semana = {}, {}
    for fila in filas:
        semana, origen, kg = fila[n_dim], fila[n_dim + 1], fila[n_dim + 2] or 0
        por_fuente[origen] = por_fuente.get(origen, 0) + kg
        por_semana.setdefault(semana, {})[origen] = por_semana.get(semana, {}).get(origen, 0) + kg

    kpis = [{"etiqueta": f"Total {nombre}", "valor": round(por_fuente.get(origen, 0)), "unidad": "kg"}
            for origen, nombre in nombres.items()]
    if "productor" in dimensiones:
        k = dimensiones.index("productor")
        kpis.append({"etiqueta": "Fundos con cosecha", "valor": len({f[k] for f in filas})})
    kpis.append({"etiqueta": "Semanas", "valor": len(semanas), "tipo": "texto"})

    pedido = grafico or {}
    por = pedido.get("por")
    if pedido.get("tipo") == "ninguno":
        dibujo = None
    elif por and por != "semana" and por in dimensiones:
        k = dimensiones.index(por)
        acumulado = {}
        for fila in filas:
            valor = fila[k]
            etiqueta = (traducir_especie(valor) if por == "especie"
                        else _formatear_grupo(valor) if por == "grupo" else str(valor or "-").strip())
            acumulado.setdefault(etiqueta, {})
            acumulado[etiqueta][fila[n_dim + 1]] = acumulado[etiqueta].get(fila[n_dim + 1], 0) + (fila[n_dim + 2] or 0)
        etiqueta_dim = DIMENSIONES_ETIQUETA.get(por, por)
        tabla_tmp = [[e] + [v.get(o, 0) for o in nombres] for e, v in acumulado.items()]
        dibujo = grafico_desde_tabla(
            [etiqueta_dim] + list(nombres.values()), tabla_tmp,
            ["texto"] + ["numero"] * len(nombres), tipo=pedido.get("tipo"),
            titulo=pedido.get("titulo") or f"Kilos proyectados por {etiqueta_dim.lower()}",
        )
    else:
        tipo = pedido.get("tipo") if pedido.get("tipo") in TIPOS_GRAFICO else (
            "lineas" if len(semanas) > 12 else "barras")
        dibujo = {
            "tipo": tipo,
            "titulo": pedido.get("titulo") or "Kilos proyectados por semana",
            "categorias": [rotulo[s] for s in semanas],
            "series": [(nombre, [por_semana.get(s, {}).get(origen, 0) for s in semanas])
                       for origen, nombre in nombres.items()],
        }
        if tipo == "torta":
            dibujo["series"] = dibujo["series"][:1]
    subtitulo = (f"Semanas {rotulo[semanas[0]].split()[0]} a {rotulo[semanas[-1]].split()[0]} · "
                 f"del {inicio.strftime('%d-%m-%Y')} al {fin.strftime('%d-%m-%Y')} · kilos")
    if filtros_desc:
        subtitulo += " · " + ", ".join(filtros_desc)
    return {
        "titulo": f"Proyección de cosecha: {_enumerar(nombres.values())}",
        "subtitulo": subtitulo,
        "resumen": resumen,
        "kpis": kpis,
        "secciones": [{"titulo": "Detalle por semana", "encabezados": encabezados, "filas": datos,
                       "total": total, "grafico": dibujo}],
        "notas": [NOTA_FUENTES],
    }

def _pivotear(encabezados, filas, pivote):
    """Tabla cruzada: pivote = {"filas": [columnas que quedan como filas], "columnas":
    columna cuyos valores pasan a ser columnas (ej. Semana), "valor": columna a sumar}.
    Agrega la columna Total."""
    idx_filas = [_buscar_columna(encabezados, c) for c in pivote.get("filas") or []]
    idx_col = _buscar_columna(encabezados, pivote.get("columnas"))
    idx_val = _buscar_columna(encabezados, pivote.get("valor"))
    if None in idx_filas or idx_col is None or idx_val is None or not idx_filas:
        raise ValueError(
            f"El pivote no calza con las columnas de la consulta ({', '.join(encabezados)})."
        )
    nombre_col = encabezados[idx_col]
    es_semana = "semana" in _palabras(nombre_col)
    valores_col = list(dict.fromkeys(f[idx_col] for f in filas))
    if es_semana:
        valores_col.sort(key=lambda v: orden_semana(v) if _es_numero(v) else 999)
    else:
        valores_col.sort(key=lambda v: (str(type(v)), v if not isinstance(v, str) else v.upper()))

    def rotulo(v):
        if es_semana and _es_numero(v):
            return f"S{etiqueta_semana(v)}"
        if hasattr(v, "strftime"):
            return v.strftime("%d-%m-%Y")
        return str(v)

    claves, acumulado = [], {}
    for fila in filas:
        clave = tuple(fila[i] for i in idx_filas)
        if clave not in acumulado:
            acumulado[clave] = {}
            claves.append(clave)
        if _es_numero(fila[idx_val]):
            acumulado[clave][fila[idx_col]] = acumulado[clave].get(fila[idx_col], 0) + fila[idx_val]
    nuevos_encabezados = [encabezados[i] for i in idx_filas] + [rotulo(v) for v in valores_col] + ["Total"]
    nuevas_filas = []
    for clave in claves:
        valores = [acumulado[clave].get(v, "") for v in valores_col]
        nuevas_filas.append(list(clave) + valores + [sum(x for x in valores if _es_numero(x))])
    return nuevos_encabezados, nuevas_filas

def _notas_de_fuente(consultas):
    """Nota al pie según las tablas que leyó el informe: recepción, embarques o ambas."""
    texto = " ".join(c or "" for c in consultas).lower()
    notas = []
    if re.search(r"\bdatos\b", texto):
        notas.append(NOTA_FUENTES)
    # Solo como tabla: "Packing" también es la columna de planta de `datos`.
    if re.search(r"\b(?:from|join)\s+\[?packing\b", texto):
        notas.append("Embalaje según Packing_Consolidado (Control EAS). Kgs Embalados = salida del "
                     "proceso; Kgs Vaciados = entrada. No incluye la fuente Packing Edge.")
    if re.search(r"\bembarques\b", texto):
        notas.append("Embarques según Reporte_Embarque_PBI (Control EAS). Cajas = cajas físicas; "
                     "Cajas Eq = cajas equivalentes.")
    if re.search(r"\bconfirmaciones\b", texto):
        notas.append("Confirmaciones y ajustes según el historial del Asistente (cuestionarios de las "
                     "08:00 y 15:00, reportes espontáneos y resúmenes a plantas). Los ajustes son lo que "
                     "informaron los zonales: no modifican la base Control EAS.")
    if re.search(r"\bembarques_detalle\b", texto):
        notas.append("Productor, planta y calibre de lo embarcado según Embarque-Liquidacion-PBI (sin valores FOB).")
    if re.search(r"\bembarques_fob\b", texto):
        notas.append("Valores FOB en USD según Embarque-Liquidacion-PBI. Incluyen embarques solo "
                     "facturados, cuyo valor es provisorio hasta la liquidación.")
    return notas or [NOTA_FUENTES]

def _consulta_informe(sql, fuente, alcance, max_filas, embarques, confirmaciones, bases):
    """Ejecuta la consulta de una sección según su fuente: la base de cosecha (SQL Server) o
    el historial de confirmaciones (SQLite, solo para los perfiles que lo ven)."""
    if fuente == "confirmaciones":
        if not confirmaciones:
            return None, "el historial de confirmaciones es solo para admin, EAS y gerencia."
        return ejecutar_consulta_confirmaciones(sql, max_filas=max_filas)
    return ejecutar_consulta_sql(sql, alcance, max_filas=max_filas, embarques=embarques, bases=bases)

def generar_informe_personalizado(numero_destino, formato, titulo, secciones, subtitulo=None,
                                  resumen=None, consulta_kpis=None, alcance=None, embarques=False,
                                  confirmaciones=False, fuente_kpis=None, bases=BASES_LECTURA["ambas"]):
    """Informe a medida que arma Claude: cada sección es una consulta SQL sobre las bases del
    usuario (y las tablas de embarques si el perfil las ve), que se ejecuta aquí, así las cifras salen de
    la base y no del modelo, con pivote y gráfico opcionales. Si ninguna sección pide
    gráfico, la primera lleva el de totales."""
    if not secciones:
        return "ERROR: el informe necesita al menos una sección con su consulta_sql."
    notas = _notas_de_fuente([consulta_kpis] + [s.get("consulta_sql") for s in secciones])
    informe = {"titulo": titulo or "Informe de cosecha", "subtitulo": subtitulo,
               "resumen": resumen, "kpis": [], "secciones": [], "notas": notas}
    if consulta_kpis:
        resultado, error = _consulta_informe(consulta_kpis, fuente_kpis, alcance, 1, embarques, confirmaciones, bases)
        if error:
            return f"ERROR en consulta_kpis: {error} No envié nada; corrígela y vuelve a intentarlo."
        if resultado["filas"]:
            for columna, valor in zip(resultado["columnas"], resultado["filas"][0]):
                valor = _valor_normalizado(valor)
                tipo = _tipo_columna(columna, [valor])
                kpi = {"etiqueta": columna, "valor": valor,
                       "tipo": tipo if tipo in ("numero", "porcentaje") else "texto",
                       "signo": _lleva_signo(columna)}
                if tipo == "razon":
                    kpi.update(tipo="numero", decimales=2)
                informe["kpis"].append(kpi)
    alguno_pide_grafico = any((s.get("grafico") or {}).get("tipo") not in (None, "ninguno")
                              for s in secciones)
    for numero_seccion, seccion in enumerate(secciones[:8]):
        nombre = seccion.get("titulo") or f"Sección {numero_seccion + 1}"
        resultado, error = _consulta_informe(seccion.get("consulta_sql"), seccion.get("fuente"), alcance,
                                             MAX_FILAS_INFORME, embarques, confirmaciones, bases)
        if error:
            return f"ERROR en la sección '{nombre}': {error} No envié nada; corrige la consulta y vuelve a intentarlo."
        encabezados = resultado["columnas"]
        filas = [[_valor_normalizado(v) for v in f] for f in resultado["filas"]]
        # La base guarda las especies en inglés; en el informe van en español.
        for i in [i for i, h in enumerate(encabezados) if _normalizar_nombre(h) == "ESPECIE"]:
            for fila in filas:
                if isinstance(fila[i], str):
                    fila[i] = traducir_especie(fila[i])
        pivoteada = False
        if seccion.get("pivote") and filas:
            try:
                encabezados, filas = _pivotear(encabezados, filas, seccion["pivote"])
                pivoteada = True
            except ValueError as e:
                return f"ERROR en la sección '{nombre}': {e}"
        # Si la consulta ya trae su fila de total (primera columna de texto = "TOTAL"), esa
        # es la fila de total: queda al pie, en negrita y fuera del gráfico. Así los promedios
        # ponderados (USD/caja eq) del total salen de la base y no de sumar promedios.
        tipos = _tipos_columnas(encabezados, filas)
        primera_texto = next((i for i, t in enumerate(tipos) if t == "texto"), None)
        totales_sql = [f for f in filas if primera_texto is not None
                       and str(f[primera_texto]).strip().upper() == "TOTAL"]
        if len(totales_sql) == 1:
            filas = [f for f in filas if f is not totales_sql[0]]
            tipos = _tipos_columnas(encabezados, filas)
            total = totales_sql[0]
        else:
            total = fila_total(encabezados, filas, tipos) if seccion.get("total", True) else None
        pedido = seccion.get("grafico") or {}
        dibujo = None
        if pedido.get("tipo") != "ninguno" and filas and (
                pedido.get("tipo") or (not alguno_pide_grafico and numero_seccion == 0)):
            if pivoteada and not pedido.get("categoria"):
                dibujo = grafico_totales_por_columna(encabezados, filas, tipos,
                                                     titulo=pedido.get("titulo"), tipo=pedido.get("tipo"))
            else:
                dibujo = grafico_desde_tabla(encabezados, filas, tipos, por=pedido.get("categoria"),
                                             valores=pedido.get("valores"), tipo=pedido.get("tipo"),
                                             titulo=pedido.get("titulo"))
        nota = seccion.get("nota")
        if resultado["truncado"]:
            nota = ((nota + " ") if nota else "") + (
                f"Se muestran las primeras {MAX_FILAS_INFORME} filas; resume más la consulta para ver todo.")
        informe["secciones"].append({"titulo": nombre, "encabezados": encabezados, "filas": filas,
                                     "total": total, "grafico": dibujo, "nota": nota})
    return enviar_informe(numero_destino, formato, informe)

def enviar_documento(numero_destino, contenido, nombre_archivo, mime, descripcion=""):
    """Sube el archivo a Meta y lo manda como documento. Igual que el texto libre, necesita
    la ventana de 24 h abierta: los archivos no se pueden mandar dentro de una plantilla."""
    try:
        subida = requests.post(
            f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/media",
            headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}"},
            files={"file": (nombre_archivo, contenido, mime)},
            data={"messaging_product": "whatsapp", "type": mime},
        )
        if subida.status_code != 200:
            logger.error(f"No pude subir {nombre_archivo} a Meta: {subida.status_code} {subida.text[:300]}")
            return False
        media_id = subida.json().get("id")
        envio = requests.post(
            f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/messages",
            headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"},
            json={
                "messaging_product": "whatsapp",
                "to": numero_destino,
                "type": "document",
                "document": {"id": media_id, "filename": nombre_archivo, "caption": descripcion[:1024]},
            },
        )
        if envio.status_code != 200:
            logger.error(f"No pude enviar {nombre_archivo} a {numero_destino}: {envio.status_code} {envio.text[:300]}")
            return False
        registrar_mensaje_enviado(envio, numero_destino)
        logger.info(f"Documento {nombre_archivo} aceptado para {numero_destino}")
        return True
    except Exception as e:
        logger.error(f"Error enviando documento: {str(e)}")
        return False

def _nombre_archivo(base, extension):
    limpio = "".join(c if c.isalnum() or c in " -_" else "" for c in base).strip()
    limpio = "_".join(limpio.split())[:60] or "consulta"
    return f"{limpio}_{datetime.now().strftime('%Y%m%d_%H%M')}.{extension}"

def enviar_informe(numero_destino, formato, informe):
    """Genera el informe en el formato pedido (PDF por defecto) y lo envía. Devuelve el
    texto que ve Claude: qué se envió y sus cifras clave, para que pueda comentarlo sin
    inventar nada. Sin número de destino (pruebas locales) lo guarda en la carpeta temporal."""
    formato = (formato or "pdf").strip().lower()
    if formato in ("xlsx", "xls", "planilla"):
        formato = "excel"
    if formato not in FORMATOS_ARCHIVO:
        formato = "pdf"
    if not informe or not any(s.get("filas") for s in informe.get("secciones", [])):
        return "No hay datos para armar el archivo con esos filtros."
    _preparar_informe(informe)
    extension, mime = FORMATOS_ARCHIVO[formato]
    try:
        contenido = (generar_excel_informe(informe) if formato == "excel"
                     else generar_pdf_informe(informe))
    except Exception as e:
        logger.exception(f"Error generando {formato}: {str(e)}")
        return "Tuve un problema armando el archivo. ¿Lo intentamos de nuevo?"
    nombre = _nombre_archivo(informe["titulo"], extension)
    if numero_destino:
        if not enviar_documento(numero_destino, contenido, nombre, mime, descripcion=informe["titulo"]):
            return "No pude enviarte el archivo. Escríbeme de nuevo y lo reintento."
        envio = f"Te envié el informe *{nombre}*"
    else:
        ruta = os.path.join(tempfile.gettempdir(), nombre)
        with open(ruta, "wb") as archivo:
            archivo.write(contenido)
        envio = f"(Prueba local sin número de destino) Informe guardado en {ruta}"
    filas = sum(len(s["filas"]) for s in informe["secciones"])
    graficos = [s["grafico"]["titulo"] for s in informe["secciones"] if s.get("grafico")]
    partes = [f"📎 {envio} ({formato.upper()}, {filas} filas)"]
    if graficos:
        partes.append(f"Gráfico: {_enumerar(graficos)}")
    if informe.get("kpis"):
        partes.append("Cifras clave: " + "; ".join(
            f"{k['etiqueta']} {_texto_kpi(k)}" for k in informe["kpis"]))
    return ". ".join(partes) + "."

def ventana_24h_abierta(numero):
    """¿La persona escribió al bot en las últimas 24 h? Si sí, se le puede mandar texto libre
    (con la tabla); si no, hay que usar una plantilla o Meta rechaza el envío con 131047."""
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        fila = conn.execute(
            "SELECT 1 FROM conversaciones WHERE numero_sender = ? "
            "AND fecha_hora >= datetime('now', 'localtime', '-1 day') LIMIT 1",
            (normalizar_numero(numero),)
        ).fetchone()
        conn.close()
        return fila is not None
    except Exception as e:
        logger.error(f"Error verificando ventana de 24 h: {str(e)}")
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
    "acceso": "acceso_total",
    "accesototal": "acceso_total",
    "todos_los_fundos": "acceso_total",
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
    acceso_total = tiene_acceso_total(num)
    ve_todo = rol in ROLES_VEN_TODO or acceso_total
    zonal = zonal_de_numero(num)
    if rol in ROLES_VEN_TODO:
        detalle_alcance = f"todos los fundos (por su perfil {rol})"
    elif rol == "planta":
        plantas_num = plantas_de(num)
        detalle_alcance = (f"lo que entra a {len(plantas_num)} planta(s): {', '.join(plantas_num)}"
                           if plantas_num else "ninguna (sin planta asignada)")
    elif acceso_total:
        detalle_alcance = "todos los fundos (acceso total activado)"
    else:
        asignados = fundos_de(num)
        efectivos = fundos_efectivos(num)
        if zonal:
            detalle_alcance = (f"{len(efectivos)} fundo(s): los {len(productores_de_zonal(zonal))} del zonal "
                               f"{zonal}" + (f" y {len(asignados)} asignado(s) a mano" if asignados else ""))
        else:
            detalle_alcance = f"{len(asignados)} fundo(s) asignados" if asignados else "ninguno (sin fundos asignados)"
    return {
        "rol": rol,
        "zonal": zonal,
        "fundos": fundos_de(num),
        "plantas": plantas_de(num),
        "embarques": NOMBRE_NIVEL_EMBARQUES[nivel_embarques(num)],
        "bases": NOMBRE_BASES[bases_de(num)],
        "ve_todo": ve_todo,
        "acceso_total": acceso_total,
        "alcance": detalle_alcance,
        "recibe_notificaciones": notif,
        "notificaciones": detalle_notif,
    }

def _embarques_param(valor):
    """Valida ?embarques= : (0 | 1 | 2 | None si no vino, error). Acepta no/sin_fob/fob (y
    0/1/2, si = fob)."""
    if valor is None:
        return None, None
    texto = valor.strip().lower()
    equivalencias = {"no": 0, "0": 0, "sin_fob": 1, "1": 1, "cajas": 1, "fob": 2, "2": 2, "si": 2, "sí": 2, "todo": 2}
    if texto not in equivalencias:
        return None, JSONResponse(
            {"status": "error", "error": "embarques debe ser no, sin_fob o fob."}, status_code=400)
    return equivalencias[texto], None

def fijar_nivel_embarques(numero, nivel):
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.execute("UPDATE numeros_permitidos SET ver_embarques = ? WHERE numero = ?", (nivel, normalizar_numero(numero)))
    conn.commit()
    conn.close()

def _bases_param(valor):
    """Valida ?bases= : (ambas | cosecha | packing | None si no vino, error)."""
    if valor is None:
        return None, None
    texto = valor.strip().lower()
    if texto not in BASES_LECTURA:
        return None, JSONResponse(
            {"status": "error", "error": "bases debe ser ambas, cosecha o packing."}, status_code=400)
    return texto, None

def fijar_bases(numero, nombre):
    conn = sqlite3.connect(DB_LOCAL_PATH)
    conn.execute("UPDATE numeros_permitidos SET ver_bases = ? WHERE numero = ?", (nombre, normalizar_numero(numero)))
    conn.commit()
    conn.close()

def _plantas_param(plantas):
    """Valida ?plantas= (separadas por "|"): (lista con los nombres exactos | [] para quitar |
    None si no vino, error)."""
    if plantas is None:
        return None, None
    pedidas = [p.strip() for p in plantas.split("|") if p.strip()]
    if not PACKINGS_CONOCIDOS:
        return pedidas, None  # el bot arrancó sin base: se aceptan tal cual
    exactas = {p.upper(): p for p in PACKINGS_CONOCIDOS}
    desconocidas = [p for p in pedidas if p.upper() not in exactas]
    if desconocidas:
        return None, JSONResponse(
            {"status": "error",
             "error": f"No conozco la planta '{desconocidas[0]}'. Plantas: {', '.join(PACKINGS_CONOCIDOS)}."},
            status_code=400,
        )
    return [exactas[p.upper()] for p in pedidas], None

def _zonal_param(zonal):
    """Valida el parámetro ?zonal= de los endpoints: (nombre_exacto | "" para desvincular |
    None si no vino, error)."""
    if zonal is None:
        return None, None
    if zonal.strip().lower() in ("", "0", "no", "ninguno", "quitar"):
        return "", None
    real = resolver_zonal(zonal)
    if not real:
        return None, JSONResponse(
            {"status": "error",
             "error": f"No conozco al zonal '{zonal}'. Zonales: {', '.join(zonales_conocidos())}. "
                      f"Para crear uno nuevo, asígnale productores con /admin/zonales/asignar."},
            status_code=400,
        )
    return real, None

@app.get("/admin/numeros/agregar")
async def admin_agregar_numero(request: Request, clave: str, numero: str, nombre: str = None,
                               rol: str = None, productor: str = None, fundos: str = None,
                               notificaciones: str = None, acceso_total: str = None, zonal: str = None,
                               plantas: str = None, embarques: str = None, bases: str = None):
    """
    Da acceso a un número (protegido con clave). Si el número ya existía, actualiza solo los
    campos que vengan en la URL. Si es nuevo, además le envía la bienvenida por WhatsApp.
    rol: admin | gerencia | eas | zonal | productor (por defecto: productor; zonal si se da ?zonal=).
    zonal: lo vincula a un zonal de /admin/zonales: ve los productores de ese zonal.
    fundos: fundos asignados separados por "|" (para zonal/productor, que solo ven los suyos).
    notificaciones: 1/si para que gerencia o eas reciba los avisos automáticos.
    bases: ambas | cosecha | packing, lo que puede consultar (sin elegir: solo cosecha).
    Uso: .../admin/numeros/agregar?clave=...&numero=56912345678&nombre=Joaquin&zonal=Joaquin Llano
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    valores = _con_alias(request, {"fundos": fundos, "notificaciones": notificaciones,
                                   "acceso_total": acceso_total})
    fundos, notificaciones, acceso_total = valores["fundos"], valores["notificaciones"], valores["acceso_total"]
    error = _revisar_parametros(
        request,
        {"clave", "numero", "nombre", "rol", "productor", "fundos", "notificaciones", "acceso_total", "zonal", "plantas", "embarques", "bases"}
        | set(ALIAS_PARAMETROS),
    )
    if error:
        return error
    if rol is not None and rol not in ROLES_VALIDOS:
        return JSONResponse(
            {"status": "error", "error": f"Rol inválido: {rol}. Válidos: {', '.join(ROLES_VALIDOS)}"},
            status_code=400,
        )
    zonal_real, error = _zonal_param(zonal)
    if error:
        return error
    plantas_lista, error = _plantas_param(plantas)
    if error:
        return error
    nivel_emb, error = _embarques_param(embarques)
    if error:
        return error
    bases_elegidas, error = _bases_param(bases)
    if error:
        return error
    try:
        notif = _parsear_si_no(notificaciones)
        acceso = _parsear_si_no(acceso_total)
    except ValueError:
        return JSONResponse({"status": "error", "error": ERROR_SI_NO}, status_code=400)
    try:
        es_nuevo = not numero_esta_permitido(numero)
        if es_nuevo and zonal_real and rol is None:
            rol = "zonal"
        # Vincularlo a un zonal lo acota a esos productores (igual que asignarle fundos),
        # salvo que en la misma llamada se pida acceso_total.
        if zonal_real and acceso is None:
            acceso = 0
        agregar_numero_permitido(numero, nombre, rol=rol, productor=productor,
                                 recibe_notificaciones=notif, acceso_total=acceso, zonal=zonal_real)
        numero_normalizado = normalizar_numero(numero)
        if fundos is not None:
            # Si en la misma llamada se pidió acceso_total, ese gana sobre la regla de
            # "asignar fundos acota".
            asignar_fundos(numero_normalizado, fundos.split("|"), reemplazar=True,
                           revocar_acceso_total=(acceso is None))
        if plantas_lista is not None:
            asignar_plantas(numero_normalizado, plantas_lista)
        if nivel_emb is not None:
            fijar_nivel_embarques(numero_normalizado, nivel_emb)
        if bases_elegidas is not None:
            fijar_bases(numero_normalizado, bases_elegidas)

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
                            productor: str = None, fundos: str = None, notificaciones: str = None,
                            acceso_total: str = None, zonal: str = None, plantas: str = None,
                            embarques: str = None, bases: str = None):
    """
    Cambia el rol, el zonal, los fundos asignados, las bases que lee y/o si recibe notificaciones.
    Roles: admin (todo + avisos siempre), gerencia y eas (consultan todo; avisos solo si
    notificaciones=1), zonal y productor (solo SUS fundos).
    zonal: lo vincula a un zonal (ve sus productores); zonal=ninguno lo desvincula.
    bases: ambas | cosecha | packing, lo que puede consultar.
    Uso: .../admin/numeros/rol?clave=...&numero=56912345678&rol=zonal&zonal=Pedro Dominguez
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    valores = _con_alias(request, {"fundos": fundos, "notificaciones": notificaciones,
                                   "acceso_total": acceso_total})
    fundos, notificaciones, acceso_total = valores["fundos"], valores["notificaciones"], valores["acceso_total"]
    error = _revisar_parametros(
        request,
        {"clave", "numero", "rol", "productor", "fundos", "notificaciones", "acceso_total", "zonal", "plantas", "embarques", "bases"}
        | set(ALIAS_PARAMETROS),
    )
    if error:
        return error
    if (rol is None and productor is None and fundos is None and notificaciones is None
            and acceso_total is None and zonal is None and plantas is None and embarques is None and bases is None):
        return JSONResponse(
            {"status": "error",
             "error": "Indica al menos rol, zonal, fundos, plantas, embarques, bases, productor, notificaciones o acceso_total"},
            status_code=400,
        )
    if rol is not None and rol not in ROLES_VALIDOS:
        return JSONResponse(
            {"status": "error", "error": f"Rol inválido: {rol}. Válidos: {', '.join(ROLES_VALIDOS)}"},
            status_code=400,
        )
    zonal_real, error = _zonal_param(zonal)
    if error:
        return error
    plantas_lista, error = _plantas_param(plantas)
    if error:
        return error
    nivel_emb, error = _embarques_param(embarques)
    if error:
        return error
    bases_elegidas, error = _bases_param(bases)
    if error:
        return error
    try:
        notif = _parsear_si_no(notificaciones)
        acceso = _parsear_si_no(acceso_total)
    except ValueError:
        return JSONResponse({"status": "error", "error": ERROR_SI_NO}, status_code=400)
    try:
        if not numero_esta_permitido(numero):
            return JSONResponse({"status": "error", "error": "Ese número no está en la lista"}, status_code=404)
        if zonal_real and acceso is None:
            acceso = 0  # vincularlo a un zonal lo acota a esos productores
        agregar_numero_permitido(numero, rol=rol, productor=productor, recibe_notificaciones=notif,
                                 acceso_total=acceso, zonal=zonal_real)
        if fundos is not None:
            # Lista separada por "|" porque los nombres de fundo llevan comas y puntos.
            asignar_fundos(numero, [f for f in fundos.split("|")], reemplazar=True,
                           revocar_acceso_total=(acceso is None))
        if plantas_lista is not None:
            asignar_plantas(numero, plantas_lista)
        if nivel_emb is not None:
            fijar_nivel_embarques(numero, nivel_emb)
        if bases_elegidas is not None:
            fijar_bases(numero, bases_elegidas)
        num = normalizar_numero(numero)
        return {"status": "ok", "numero": num, **_estado_numero(num)}
    except Exception as e:
        logger.error(f"Error cambiando rol de número: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/numeros/fundos")
async def admin_fundos(request: Request, clave: str, numero: str, agregar: str = None, quitar: str = None):
    """
    Agrega o quita fundos de un número sin tocar el resto (varios separados por "|").
    Solo afecta a zonal y productor: admin, gerencia y EAS ven todo igual.
    Uso: .../admin/numeros/fundos?clave=...&numero=56912345678&agregar=SANTA ANA DE HUIQUE
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    error = _revisar_parametros(request, {"clave", "numero", "agregar", "quitar"})
    if error:
        return error
    if not agregar and not quitar:
        return JSONResponse({"status": "error", "error": "Indica agregar y/o quitar"}, status_code=400)
    try:
        if not numero_esta_permitido(numero):
            return JSONResponse({"status": "error", "error": "Ese número no está en la lista"}, status_code=404)
        if agregar:
            asignar_fundos(numero, agregar.split("|"))  # acotar implica quitar el acceso total
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

def _productores_con_movimiento():
    """{productor: última temporada} de los productores con datos en la temporada vigente o
    la anterior, para detectar los que se quedaron sin zonal."""
    conn = conectar_sql()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Productor, MAX(Temporada) FROM [Recepcion_Consolidada] "
            "WHERE Temporada >= ? AND Productor IS NOT NULL GROUP BY Productor",
            [(TEMPORADA_ACTUAL or temporada_de(date.today())) - 1],
        )
        return {fila[0].strip(): fila[1] for fila in cursor.fetchall() if fila[0] and fila[0].strip()}
    finally:
        conn.close()

@app.get("/admin/zonales")
async def admin_zonales(clave: str):
    """
    Asignación de productores a zonales: los productores de cada zonal (y si ya existen en la
    base), los números vinculados a cada uno, y los productores con movimiento reciente que
    todavía no tienen zonal.
    Uso: https://bot-whatsapp-asa.com/admin/zonales?clave=...
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        conocidos = {_normalizar_nombre(p) for p in PRODUCTORES_CONOCIDOS}
        zonales = {}
        for fila in detalle_zonales():
            zonales.setdefault(fila["zonal"], {"productores": [], "numeros": []})
            zonales[fila["zonal"]]["productores"].append({
                "productor": fila["productor"], "csg": fila["csg"], "grupo": fila["grupo"],
                "en_la_base": _normalizar_nombre(fila["productor"]) in conocidos,
            })
        conn = sqlite3.connect(DB_LOCAL_PATH)
        for numero, nombre, rol, zonal in conn.execute(
                "SELECT numero, nombre, rol, zonal FROM numeros_permitidos WHERE zonal IS NOT NULL"):
            zonales.setdefault(zonal, {"productores": [], "numeros": []})["numeros"].append(
                {"numero": numero, "nombre": nombre, "rol": rol})
        conn.close()
        asignados = {_normalizar_nombre(p) for p in asignacion_zonales()}
        movimiento = _productores_con_movimiento()
        sin_zonal = None
        if movimiento is not None:
            sin_zonal = [{"productor": p, "ultima_temporada": t} for p, t in sorted(movimiento.items())
                         if _normalizar_nombre(p) not in asignados]
        return {
            "zonales": {z: {**d, "total_productores": len(d["productores"])} for z, d in sorted(zonales.items())},
            "sin_zonal_con_movimiento_reciente": sin_zonal,
        }
    except Exception as e:
        logger.error(f"Error listando zonales: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/zonales/asignar")
async def admin_zonales_asignar(request: Request, clave: str, productor: str, zonal: str,
                                grupo: str = None, csg: str = None):
    """
    Asigna (o cambia) el zonal de un productor. Varios productores separados por "|". Si el
    zonal no existe se crea. El nombre del productor se ajusta al exacto de la base.
    Uso: .../admin/zonales/asignar?clave=...&productor=FUNDO SANTO DOMINGO&zonal=José Luis Castro
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    error = _revisar_parametros(request, {"clave", "productor", "zonal", "grupo", "csg"})
    if error:
        return error
    if not zonal.strip():
        return JSONResponse({"status": "error", "error": "Indica el zonal"}, status_code=400)
    existente = resolver_zonal(zonal)
    zonal_real = existente or " ".join(zonal.split())
    conocidos = _productores_conocidos_normalizados()
    resultado = []
    try:
        conn = sqlite3.connect(DB_LOCAL_PATH)
        for nombre in [p.strip() for p in productor.split("|") if p.strip()]:
            real = conocidos.get(_normalizar_nombre(nombre), nombre)
            conn.execute(
                "INSERT INTO productores_zonales (productor, csg, grupo, zonal) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(productor) DO UPDATE SET zonal = excluded.zonal, "
                "csg = COALESCE(excluded.csg, productores_zonales.csg), "
                "grupo = COALESCE(excluded.grupo, productores_zonales.grupo), "
                "fecha_actualizacion = datetime('now', 'localtime')",
                (real, csg, grupo, zonal_real),
            )
            resultado.append({"productor": real, "en_la_base": real in PRODUCTORES_CONOCIDOS})
        conn.commit()
        conn.close()
        return {"status": "ok", "zonal": zonal_real, "zonal_nuevo": existente is None,
                "productores": resultado, "total_del_zonal": len(productores_de_zonal(zonal_real))}
    except Exception as e:
        logger.error(f"Error asignando zonal: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/admin/zonales/quitar")
async def admin_zonales_quitar(request: Request, clave: str, productor: str):
    """
    Deja a un productor sin zonal (varios separados por "|").
    Uso: .../admin/zonales/quitar?clave=...&productor=LOS LOROS LIMITADA
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    error = _revisar_parametros(request, {"clave", "productor"})
    if error:
        return error
    try:
        asignados = {_normalizar_nombre(p): p for p in asignacion_zonales()}
        conn = sqlite3.connect(DB_LOCAL_PATH)
        quitados, no_encontrados = [], []
        for nombre in [p.strip() for p in productor.split("|") if p.strip()]:
            real = asignados.get(_normalizar_nombre(nombre))
            if real:
                conn.execute("DELETE FROM productores_zonales WHERE productor = ?", (real,))
                quitados.append(real)
            else:
                no_encontrados.append(nombre)
        conn.commit()
        conn.close()
        return {"status": "ok" if quitados else "error", "quitados": quitados,
                "no_encontrados": no_encontrados}
    except Exception as e:
        logger.error(f"Error quitando zonal: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

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

@app.get("/admin/estado")
async def admin_estado(clave: str):
    """
    Resumen de cómo quedó el bot tras un reinicio: modo de envíos, tareas programadas con su
    próxima ejecución, plantillas configuradas, perfiles y entregas recientes.
    Uso: https://bot-whatsapp-asa.com/admin/estado?clave=...
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        tareas = []
        for j in (scheduler.get_jobs() if scheduler.running else []):
            tareas.append({
                "tarea": j.id,
                "proxima_ejecucion": j.next_run_time.strftime("%Y-%m-%d %H:%M") if j.next_run_time else None,
            })

        conn = sqlite3.connect(DB_LOCAL_PATH)
        conn.row_factory = sqlite3.Row
        perfiles = {r["rol"] or ROL_POR_DEFECTO: r["n"] for r in conn.execute(
            "SELECT rol, COUNT(*) n FROM numeros_permitidos GROUP BY rol"
        )}
        entregas = {r["estado"]: r["n"] for r in conn.execute(
            "SELECT estado, COUNT(*) n FROM mensajes_estado GROUP BY estado"
        )}
        con_acceso_total = conn.execute(
            "SELECT COUNT(*) FROM numeros_permitidos WHERE acceso_total = 1"
        ).fetchone()[0]
        sin_fundos = conn.execute(
            f"SELECT COUNT(*) FROM numeros_permitidos WHERE rol IN ({','.join('?' * len(ROLES_ACOTADOS))}) "
            "AND acceso_total = 0 AND numero NOT IN (SELECT numero FROM numeros_fundos)",
            list(ROLES_ACOTADOS)
        ).fetchone()[0]
        conn.close()

        return {
            "modo_envios": _modo_envios(),
            "scheduler_activo": scheduler.running,
            "tareas_programadas": tareas,
            "horarios": {
                "cuestionario_manana": CUESTIONARIO_HORA_AM,
                "cuestionario_tarde": CUESTIONARIO_HORA_PM,
                "resumen_semanal": f"{RESUMEN_SEMANAL_DIA} {RESUMEN_SEMANAL_HORA}",
                "alerta_desviaciones": f"{ALERTA_SEMANAL_DIA} {ALERTA_SEMANAL_HORA}",
            },
            "plantillas": {
                "cuestionario": f"{PLANTILLA_CUESTIONARIO_NOMBRE} ({PLANTILLA_CUESTIONARIO_IDIOMA})",
                "bienvenida": f"{PLANTILLA_BIENVENIDA_NOMBRE} ({PLANTILLA_BIENVENIDA_IDIOMA})",
            },
            "fuente_cuestionario": BASE_ORIGEN_CUESTIONARIO,
            "perfiles": perfiles,
            "acotados_sin_fundos": sin_fundos,
            "con_acceso_total": con_acceso_total,
            "entregas_por_estado": entregas,
        }
    except Exception as e:
        logger.error(f"Error en /admin/estado: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

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

@app.get("/admin/plantas")
async def admin_plantas(clave: str):
    """Plantas (Packing) que conoce el bot, con su nombre exacto, para asignarlas al perfil planta."""
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    return {"total": len(PACKINGS_CONOCIDOS), "plantas": sorted(PACKINGS_CONOCIDOS)}

@app.get("/admin/plantas/resumen")
async def admin_resumen_plantas(clave: str, solo_ver: bool = False, fecha: str = None, planta: str = None):
    """
    Dispara el resumen de cierre a las plantas (mismo envío que el job de las 18:00, respeta
    el modo prueba/producción). Con solo_ver=true muestra el texto sin enviar; planta=Nombre
    (o varias separadas por "|") lo acota; fecha=YYYY-MM-DD elige el día.
    Uso: .../admin/plantas/resumen?clave=...&solo_ver=true&planta=LISONJERA
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    try:
        dia = _parsear_fecha_param(fecha)
    except ValueError:
        return JSONResponse({"status": "error", "error": "fecha debe ser YYYY-MM-DD"}, status_code=400)
    if solo_ver:
        plantas_lista, error = _plantas_param(planta)
        if error:
            return error
        texto, kg, conteo = construir_resumen_planta(dia, plantas_lista, nombre="Edson")
        return {"status": "ok", "texto": texto or "(sin previsto ese día: no se enviaría nada)",
                "kg": kg, "conteo": conteo}
    return {"status": "ok", "modo": _modo_envios(), "resultado": enviar_resumen_plantas(fecha=dia)}

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
def test_mensaje(clave: str, mensaje: str, numero: str = None, guardar: bool = False):
    """
    PRUEBA: envía un mensaje y obtiene la respuesta, con las herramientas que usó Claude.
    Pide la clave de administración: Caddy publica todo el puerto 8005, y sin número la
    consulta tiene acceso total (incluidos embarques y FOB).
    Usa: curl "http://localhost:8005/test/mensaje?clave=...&mensaje=cuantos%20bins%20de%20tiffany"
    Para probar contexto entre mensajes: pasa el mismo &numero=... y &guardar=true en cada llamada.
    OJO: con &numero=... los informes pedidos se envían de verdad por WhatsApp a ese número.
    """
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
    traza = []
    respuesta = procesar_mensaje(mensaje, numero, traza=traza)
    if guardar and numero:
        guardar_conversacion(numero, "text", mensaje, respuesta, herramientas=traza)
    return {"pregunta": mensaje, "respuesta": respuesta, "herramientas": traza}

@app.get("/test/conexion")
async def test_conexion(clave: str):
    """PRUEBA: verifica la conexión a SQL Server (con la clave de administración)."""
    if clave != ADMIN_CLAVE:
        return JSONResponse({"status": "error", "error": "Clave inválida"}, status_code=403)
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
