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
from datetime import datetime
from dotenv import load_dotenv
import logging
import anthropic
import openai

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
    conn.commit()
    conn.close()

inicializar_db_local()

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
# 'Trisemanal' = estimación de cosecha | 'Recepción Planta' = cosecha real
# Verificado contra ejemplo real: W. Murcott 2026-08-12 -> Trisemanal=160.300kg, Recepción Planta=162.136kg (+1.15%)
BASE_ORIGEN_ESTIMADO = "Trisemanal"
BASE_ORIGEN_REAL = "Recepción Planta"

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

def obtener_bins_estimados(variedad):
    """Consulta: ¿Cuántos kg se estiman cosechar esta temporada de [variedad]? (fuente: Trisemanal)"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        query = """
        SELECT SUM(KgsRecepcionados) as total
        FROM [Recepcion_Consolidada]
        WHERE Variedad LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
        """
        cursor.execute(query, (f"%{variedad}%", BASE_ORIGEN_ESTIMADO))
        resultado = cursor.fetchone()
        conn.close()

        if resultado and resultado[0]:
            return f"📦 {variedad.upper()}: {formatear_kg(resultado[0])} kg estimados esta temporada (trisemanal)"
        else:
            return f"No hay estimación trisemanal registrada para {variedad}"
    except Exception as e:
        logger.error(f"Error en obtener_bins_estimados: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_cosecha_actual(variedad, fecha=None):
    """Consulta: ¿Cuántos kg se cosecharon (real) de [variedad] en una fecha? (default: hoy). fecha en formato YYYY-MM-DD"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        query = """
        SELECT SUM(KgsRecepcionados) as total
        FROM [Recepcion_Consolidada]
        WHERE Variedad LIKE ?
        AND [Base Origen] = ?
        AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE))
        """
        cursor.execute(query, (f"%{variedad}%", BASE_ORIGEN_REAL, fecha))
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

def obtener_comparacion_estimado_vs_cosechado(variedad, fecha=None):
    """Consulta: ¿Cómo vamos de [variedad] respecto a lo estimado, en una fecha? (default: hoy). fecha en formato YYYY-MM-DD"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        cursor.execute(
            """SELECT SUM(KgsRecepcionados) FROM [Recepcion_Consolidada]
               WHERE Variedad LIKE ? AND [Base Origen] = ?
               AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE))""",
            (f"%{variedad}%", BASE_ORIGEN_ESTIMADO, fecha)
        )
        fila = cursor.fetchone()
        estimado = fila[0] if fila and fila[0] else 0

        cursor.execute(
            """SELECT SUM(KgsRecepcionados) FROM [Recepcion_Consolidada]
               WHERE Variedad LIKE ? AND [Base Origen] = ?
               AND CAST(Fecha AS DATE) = COALESCE(?, CAST(GETDATE() AS DATE))""",
            (f"%{variedad}%", BASE_ORIGEN_REAL, fecha)
        )
        fila = cursor.fetchone()
        real = fila[0] if fila and fila[0] else 0

        conn.close()

        etiqueta_fecha = fecha if fecha else "hoy"

        if not estimado and not real:
            return f"No hay datos (ni estimado ni cosecha real) de {variedad} para {etiqueta_fecha}"

        if estimado and real:
            porcentaje = ((real - estimado) / estimado) * 100
            signo = "+" if porcentaje >= 0 else ""
            return (
                f"📊 {variedad.upper()} ({etiqueta_fecha}): estimado {formatear_kg(estimado)} kg, "
                f"real {formatear_kg(real)} kg ({signo}{porcentaje:.1f}% vs estimado)"
            )
        elif estimado and not real:
            return f"📊 {variedad.upper()} ({etiqueta_fecha}): estimado {formatear_kg(estimado)} kg, aún sin cosecha real registrada"
        else:
            return f"📊 {variedad.upper()} ({etiqueta_fecha}): cosecha real {formatear_kg(real)} kg, no había estimación trisemanal para esa fecha"
    except Exception as e:
        logger.error(f"Error en obtener_comparacion_estimado_vs_cosechado: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_resumen_por_productor(productor):
    """Consulta: ¿Cuánto ha cosechado (real) el productor [productor] esta temporada?"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        query = """
        SELECT SUM(KgsRecepcionados) as total, COUNT(DISTINCT Variedad) as variedades
        FROM [Recepcion_Consolidada]
        WHERE Productor LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
        """
        cursor.execute(query, (f"%{productor}%", BASE_ORIGEN_REAL))
        resultado = cursor.fetchone()
        conn.close()

        if resultado and resultado[0]:
            return f"👨‍🌾 {productor.upper()}: {formatear_kg(resultado[0])} kg cosechados (real) en total, en {resultado[1]} variedades"
        else:
            return f"No encontré cosecha real registrada del productor '{productor}'"
    except Exception as e:
        logger.error(f"Error en obtener_resumen_por_productor: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_resumen_por_packing(packing):
    """Consulta: ¿Cuánto ha recibido (real) el packing [packing] esta temporada?"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        query = """
        SELECT SUM(KgsRecepcionados) as total, COUNT(DISTINCT Variedad) as variedades
        FROM [Recepcion_Consolidada]
        WHERE Packing LIKE ?
        AND [Base Origen] = ?
        AND Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
        """
        cursor.execute(query, (f"%{packing}%", BASE_ORIGEN_REAL))
        resultado = cursor.fetchone()
        conn.close()

        if resultado and resultado[0]:
            return f"🏭 Packing {packing.upper()}: {formatear_kg(resultado[0])} kg recibidos (real), en {resultado[1]} variedades"
        else:
            return f"No encontré cosecha real registrada del packing '{packing}'"
    except Exception as e:
        logger.error(f"Error en obtener_resumen_por_packing: {str(e)}")
        return f"Error al consultar: {str(e)}"

def formatear_cosecha_detalle(filas, fecha_inicio, fecha_fin, filtro_desc=""):
    """
    filas: lista de tuplas (Especie, Variedad, Productor, total_kg).
    Arma un texto agrupado por especie -> variedad, con detalle por productor
    si no son demasiadas filas (para no saturar el mensaje de WhatsApp).
    """
    if not filas:
        return None

    total_general = sum(f[3] for f in filas if f[3])

    por_especie = {}
    for especie, variedad, productor, total in filas:
        if not total:
            continue
        por_especie.setdefault(especie, {}).setdefault(variedad, []).append((productor, total))

    rango = fecha_inicio if fecha_inicio == fecha_fin else f"{fecha_inicio} a {fecha_fin}"
    lineas = [f"📅 Cosecha real {rango}{filtro_desc}:"]

    mostrar_productores = len(filas) <= 15

    for especie, variedades in por_especie.items():
        especie_es = traducir_especie(especie)
        total_especie = sum(t for vs in variedades.values() for _, t in vs)
        lineas.append(f"\n🍃 {especie_es}: {formatear_kg(total_especie)} kg")
        for variedad, productores in variedades.items():
            total_variedad = sum(t for _, t in productores)
            if mostrar_productores:
                detalle_prod = ", ".join(
                    f"{p}: {formatear_kg(t)} kg" for p, t in sorted(productores, key=lambda x: -x[1])
                )
                lineas.append(f"  • {variedad}: {formatear_kg(total_variedad)} kg ({detalle_prod})")
            else:
                lineas.append(f"  • {variedad}: {formatear_kg(total_variedad)} kg ({len(productores)} productores)")

    lineas.append(f"\n📦 Total: {formatear_kg(total_general)} kg")
    return "\n".join(lineas)

def obtener_cosecha_detalle(fecha_inicio, fecha_fin=None, especie=None, variedad=None, productor=None):
    """
    Detalle de cosecha REAL entre fecha_inicio y fecha_fin (o solo fecha_inicio si no hay fecha_fin),
    agrupado por especie, variedad y productor, con totales. Filtros opcionales.
    """
    try:
        if not fecha_fin:
            fecha_fin = fecha_inicio

        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        condiciones = ["[Base Origen] = ?", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
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

        where = " AND ".join(condiciones)
        query = f"""
            SELECT Especie, Variedad, Productor, SUM(KgsRecepcionados) as total
            FROM [Recepcion_Consolidada]
            WHERE {where}
            GROUP BY Especie, Variedad, Productor
            HAVING SUM(KgsRecepcionados) > 0
            ORDER BY Especie, Variedad, total DESC
        """
        cursor.execute(query, params)
        filas = cursor.fetchall()
        conn.close()

        resultado = formatear_cosecha_detalle(filas, fecha_inicio, fecha_fin, filtro_desc)
        if not resultado:
            return f"No hay cosecha real registrada{filtro_desc} entre {fecha_inicio} y {fecha_fin}"
        return resultado
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_detalle: {str(e)}")
        return f"Error al consultar: {str(e)}"

def obtener_ultima_cosecha(especie=None, variedad=None):
    """Encuentra la última fecha con cosecha real de una especie o variedad, y muestra su detalle"""
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()
        condiciones = ["[Base Origen] = ?", "KgsRecepcionados > 0"]
        params = [BASE_ORIGEN_REAL]
        if especie:
            condiciones.append("Especie LIKE ?")
            params.append(f"%{especie}%")
        if variedad:
            condiciones.append("Variedad LIKE ?")
            params.append(f"%{variedad}%")
        where = " AND ".join(condiciones)

        cursor.execute(f"SELECT MAX(CAST(Fecha AS DATE)) FROM [Recepcion_Consolidada] WHERE {where}", params)
        fila = cursor.fetchone()
        conn.close()

        ultima_fecha = fila[0] if fila else None
        if not ultima_fecha:
            referencia = especie or variedad or ""
            return f"No encontré cosecha real registrada de {referencia}" if referencia else "No encontré cosecha real registrada"

        fecha_str = ultima_fecha.strftime("%Y-%m-%d") if hasattr(ultima_fecha, "strftime") else str(ultima_fecha)
        return obtener_cosecha_detalle(fecha_str, fecha_str, especie=especie, variedad=variedad)
    except Exception as e:
        logger.error(f"Error en obtener_ultima_cosecha: {str(e)}")
        return f"Error al consultar: {str(e)}"

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

def normalizar_variedad(variedad_usuario):
    """Limpia espacios; la traducción al nombre exacto ya la hace Claude usando VARIEDADES_CONOCIDAS"""
    return variedad_usuario.strip()

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
        "description": "Compara lo cosechado REAL contra lo estimado (trisemanal) de una variedad, en una fecha específica, con porcentaje de avance. Usar cuando pregunten 'cómo vamos' de una variedad en una fecha dada (hoy, ayer, mañana, una fecha puntual, etc). Si hay estimado y real, muestra ambos.",
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
        "description": "Encuentra cuándo fue la última fecha con cosecha REAL registrada de una especie o variedad, y muestra el detalle de esa fecha (variedades, productores, totales). Usar para preguntas como '¿cuándo fue la última cosecha de mandarinas?'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "especie": {
                    "type": "string",
                    "description": "Especie mencionada por el usuario, traducida al nombre EXACTO en inglés de la lista de especies conocidas (ej. 'mandarina' -> 'MANDARIN'). Opcional si se da variedad."
                },
                "variedad": {
                    "type": "string",
                    "description": "Variedad específica mencionada por el usuario. Opcional si se da especie."
                }
            }
        }
    },
    {
        "name": "consultar_cosecha_detalle",
        "description": "Consulta el detalle de cosecha REAL entre dos fechas (o una sola fecha), agrupado por especie, variedad y productor, con totales. Usar para preguntas como '¿qué se cosechó ayer?', '¿cuánto se cosechó entre el 1 y el 15 de agosto?', opcionalmente filtrado por especie, variedad o productor.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fecha_inicio": {
                    "type": "string",
                    "description": "Fecha de inicio en formato YYYY-MM-DD, calculada a partir de la fecha de hoy y lo que diga el usuario."
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
                }
            },
            "required": ["fecha_inicio"]
        }
    },
]

def construir_system_prompt():
    if VARIEDADES_CONOCIDAS:
        lista_variedades = ", ".join(VARIEDADES_CONOCIDAS)
    else:
        lista_variedades = "(lista no disponible por ahora, usa el nombre tal como lo escriba el usuario)"

    lista_especies = ", ".join(f"{en} ({es})" for en, es in ESPECIE_TRADUCCION.items())

    hoy = datetime.now().strftime("%Y-%m-%d (%A)")

    return f"""Eres el asistente de WhatsApp de Agua Santa para consultas de cosecha de fruta.

Hoy es {hoy}. Usa esta fecha como referencia para calcular fechas relativas que mencione el usuario
("ayer", "hoy", "mañana", "el lunes pasado", "el 12 de agosto", "entre el 1 y el 15 de agosto", etc.)
y pásalas a las herramientas en formato YYYY-MM-DD.

Tienes herramientas para consultar, por variedad: estimado de temporada (trisemanal), cosecha real en
una fecha, calibre promedio, y comparación de avance (estimado vs cosechado real) en una fecha.
También puedes consultar resúmenes por productor o por packing (no requieren variedad), cuándo fue la
última cosecha real de una especie o variedad, y el detalle de cosecha real entre un rango de fechas
(agrupado por especie/variedad/productor, opcionalmente filtrado por especie, variedad o productor).

Usa la herramienta que corresponda cuando el usuario pregunte por alguno de esos datos y haya mencionado
(o puedas inferir) el dato que falta (variedad, especie, productor, packing, fecha o rango de fechas).

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

Si el usuario saluda, pide ayuda, o pregunta algo que no corresponde a ninguna herramienta, respóndele tú
directamente: breve, amable, en español, y si corresponde explícale qué puedes hacer.

Si falta la variedad, especie, productor o packing para poder consultar, pídeselo al usuario en vez de
inventarlo."""

def ejecutar_tool(tool_name, tool_input):
    if tool_name == "consultar_resumen_productor":
        return obtener_resumen_por_productor(tool_input.get("productor", ""))
    if tool_name == "consultar_resumen_packing":
        return obtener_resumen_por_packing(tool_input.get("packing", ""))

    if tool_name == "consultar_ultima_cosecha":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_ultima_cosecha(especie=especie, variedad=variedad_op)

    if tool_name == "consultar_cosecha_detalle":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_cosecha_detalle(
            fecha_inicio=tool_input.get("fecha_inicio"),
            fecha_fin=tool_input.get("fecha_fin") or None,
            especie=especie,
            variedad=variedad_op,
            productor=tool_input.get("productor") or None,
        )

    variedad = normalizar_variedad(tool_input.get("variedad", ""))
    fecha = tool_input.get("fecha") or None
    if tool_name == "consultar_bins_estimados":
        return obtener_bins_estimados(variedad)
    elif tool_name == "consultar_cosecha_hoy":
        return obtener_cosecha_actual(variedad, fecha)
    elif tool_name == "consultar_calibre_promedio":
        return obtener_calibre_promedio(variedad)
    elif tool_name == "comparar_estimado_vs_cosechado":
        return obtener_comparacion_estimado_vs_cosechado(variedad, fecha)
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
            system=construir_system_prompt(),
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
                        msg_type = message.get("type")

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
                        respuesta = procesar_mensaje(msg_text)

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
    Uso: https://bot-whatsapp-asa.com/historial?clave=TU_CLAVE&limit=50
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
