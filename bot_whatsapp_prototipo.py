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
# 'Trisemanal' = estimación de cosecha | 'Recepción Planta' = cosecha real
# Verificado contra ejemplo real: W. Murcott 2026-08-12 -> Trisemanal=160.300kg, Recepción Planta=162.136kg (+1.15%)
BASE_ORIGEN_ESTIMADO = "Trisemanal"
BASE_ORIGEN_REAL = "Recepción Planta"
BASE_ORIGEN_ESTIM_INVIERNO = "Estim Invierno"
BASE_ORIGEN_ESTIM_PRIMAVERA = "Estim Primavera"

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

    anchos = {"variedad": 14, "num": 11}
    header = (
        f"{'Variedad':<{anchos['variedad']}}{'Invierno':>{anchos['num']}}"
        f"{'Primavera':>{anchos['num']}}{'Real':>{anchos['num']}}"
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

    resumen = [f"\n📦 Totales: Invierno {formatear_kg(tot_inv)} {unidad} · Primavera {formatear_kg(tot_prim)} {unidad} · Real {formatear_kg(tot_real)} {unidad}"]
    for texto in [
        variacion(tot_real, tot_inv, "Real vs Invierno"),
        variacion(tot_real, tot_prim, "Real vs Primavera"),
        variacion(tot_prim, tot_inv, "Primavera vs Invierno"),
    ]:
        if texto:
            resumen.append(texto)

    lineas.append("\n".join(resumen))
    return "\n".join(lineas)

def obtener_comparativo_estimaciones(especie=None, variedad=None, productor=None, packing=None, fecha_inicio=None, fecha_fin=None, envase=None, temporada=None):
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
            if temporada:
                cursor.execute(
                    "SELECT MIN(CAST(Fecha AS DATE)), MAX(CAST(Fecha AS DATE)) FROM [Recepcion_Consolidada] WHERE Temporada = ?",
                    (temporada,)
                )
            else:
                cursor.execute("""
                    SELECT MIN(CAST(Fecha AS DATE)), MAX(CAST(Fecha AS DATE))
                    FROM [Recepcion_Consolidada]
                    WHERE Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
                """)
            fila = cursor.fetchone()
            if not fila or not fila[0]:
                conn.close()
                return "No pude determinar el rango de esa temporada. ¿Puedes darme una fecha o rango específico?"
            fecha_inicio = fecha_inicio or fila[0].strftime("%Y-%m-%d")
            fecha_fin = fecha_fin or fila[1].strftime("%Y-%m-%d")

        try:
            datetime.strptime(fecha_inicio, "%Y-%m-%d")
            datetime.strptime(fecha_fin, "%Y-%m-%d")
        except (ValueError, TypeError):
            conn.close()
            return "No entendí el rango de fechas. ¿Puedes indicarlo como 'entre el DD-MM-YYYY y el DD-MM-YYYY'?"

        condiciones = ["[Base Origen] IN (?, ?, ?)", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [BASE_ORIGEN_ESTIM_INVIERNO, BASE_ORIGEN_ESTIM_PRIMAVERA, BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
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

def formatear_cosecha_detalle(filas, fecha_inicio, fecha_fin, filtro_desc="", mostrar_fechas=True, unidad="kg"):
    """
    filas: lista de tuplas (Packing, Productor, Especie, Variedad, Fecha, Base Origen, total).
    Si el rango es corto y hay pocos grupos, arma una tabla de fecha x (estimado, real) por
    cada packing/productor/variedad. Si no, colapsa a una tabla única con columnas
    Planta | Productor | Especie | Variedad | Estimado | Real.
    unidad: etiqueta del total (ej. "kg", "BINS", "TOTES") según qué columna se sumó.
    """
    if not filas:
        return None

    grupos = {}  # (packing, productor, especie, variedad) -> {fecha: {"estimado":x, "real":y}}
    for packing, productor, especie, variedad, fecha, base_origen, total in filas:
        if not total:
            continue
        # Normaliza mayúsculas: la misma planta/productor puede venir escrito distinto
        # según si la fila es 'Trisemanal' o 'Recepción Planta' en la base de origen.
        clave = (
            (packing or "").strip().upper(),
            (productor or "").strip().upper(),
            (especie or "").strip().upper(),
            (variedad or "").strip().upper(),
        )
        clave_valor = "estimado" if base_origen == BASE_ORIGEN_ESTIMADO else "real"
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

            filas_tabla = [f"{'Fecha':<11}{'Trisem.':>9}{'Real':>9}"]
            for fecha in sorted(fechas.keys()):
                vals = fechas[fecha]
                est = vals.get("estimado", 0) or 0
                real = vals.get("real", 0) or 0
                est_str = formatear_kg(est) if est else "-"
                real_str = formatear_kg(real) if real else "-"
                filas_tabla.append(f"{_fecha_str(fecha):<11}{est_str:>9}{real_str:>9}")
            filas_tabla.append("-" * 29)
            filas_tabla.append(f"{'TOTAL':<11}{formatear_kg(total_est):>9}{formatear_kg(total_real):>9}")

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

        anchos = {"especie": 8, "variedad": 12, "num": 10}
        ancho_total = anchos["especie"] + anchos["variedad"] + anchos["num"] * 2
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
                f"{'Estimado':>{anchos['num']}}{'Real':>{anchos['num']}}"
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
        f"\n📦 Total general: estimado {formatear_kg(total_estimado_gral)} {unidad}, "
        f"real {formatear_kg(total_real_gral)} {unidad}"
    )
    return "\n".join(lineas)

def obtener_cosecha_detalle(fecha_inicio=None, fecha_fin=None, especie=None, variedad=None, productor=None, packing=None, grupo=None, forzar_fechas=False, envase=None, temporada=None):
    """
    Detalle de cosecha entre fecha_inicio y fecha_fin (o solo fecha_inicio si no hay fecha_fin),
    con columnas de estimado (Trisemanal) y real (Recepción Planta) por fecha, agrupado por
    packing/productor/variedad. Filtros opcionales. Si el rango es muy amplio o hay muchos
    grupos, se colapsa a solo totales para no saturar el mensaje.
    Si no se da fecha_inicio, se usa el inicio de la temporada vigente hasta hoy (para
    preguntas tipo "toda la temporada", "hasta hoy", sin fechas explícitas). Si se da temporada
    (ej. 2025 para "la temporada pasada") sin fechas, se usa el rango completo de esa temporada.
    Si se da envase (ej. "BINS", "TOTES", "CAJA EQ"), se filtra por ese tipo de envase y se
    suma la cantidad real de unidades (Bultos) en vez de kilos, sin inventar factores de
    conversión (los factores kg/unidad no son confiables en los datos históricos).
    """
    try:
        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        if not fecha_inicio:
            if temporada:
                cursor.execute(
                    "SELECT MIN(CAST(Fecha AS DATE)), MAX(CAST(Fecha AS DATE)) FROM [Recepcion_Consolidada] WHERE Temporada = ?",
                    (temporada,)
                )
                fila = cursor.fetchone()
                if not fila or not fila[0]:
                    conn.close()
                    return f"No encontré datos para la temporada {temporada}."
                fecha_inicio = fila[0].strftime("%Y-%m-%d")
                fecha_fin = fila[1].strftime("%Y-%m-%d")
            else:
                cursor.execute("""
                    SELECT MIN(CAST(Fecha AS DATE))
                    FROM [Recepcion_Consolidada]
                    WHERE Temporada = (SELECT MAX(Temporada) FROM [Recepcion_Consolidada] WHERE Fecha <= GETDATE())
                """)
                fila = cursor.fetchone()
                if not fila or not fila[0]:
                    conn.close()
                    return "No pude determinar el inicio de temporada. ¿Puedes darme una fecha o rango específico?"
                fecha_inicio = fila[0].strftime("%Y-%m-%d")
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
        params = [BASE_ORIGEN_ESTIMADO, BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
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

        resultado = formatear_cosecha_detalle(filas, fecha_inicio, fecha_fin, filtro_desc, mostrar_fechas, unidad)
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
}
DIMENSIONES_ETIQUETA = {
    "especie": "Especie",
    "variedad": "Variedad",
    "productor": "Productor",
    "packing": "Planta",
    "grupo": "Grupo",
    "fecha": "Fecha",
}
MAX_FILAS_FLEXIBLE = 60

def formatear_cosecha_flexible(filas, dimensiones, fecha_inicio, fecha_fin, filtro_desc="", unidad="kg"):
    """
    filas: tuplas (valor_dim1, valor_dim2, ..., Base Origen, total), según 'dimensiones'.
    Arma UNA sola tabla con columnas = dimensiones pedidas + Estimado + Real, y fila TOTAL.
    """
    if not filas:
        return None

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
        tipo = "estimado" if base_origen == BASE_ORIGEN_ESTIMADO else "real"
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
        lineas.append(f"\n📦 Total: estimado {formatear_kg(tot_est)} {unidad}, real {formatear_kg(tot_real)} {unidad}")
        return "\n".join(lineas)

    anchos_dim = []
    for d in dimensiones:
        if d == "fecha":
            anchos_dim.append(11)
        elif d in ("productor", "packing"):
            anchos_dim.append(16)
        else:
            anchos_dim.append(13)
    ancho_num = 11

    header = "".join(
        f"{DIMENSIONES_ETIQUETA.get(d, d):<{a}}" for d, a in zip(dimensiones, anchos_dim)
    )
    header += f"{'Estimado':>{ancho_num}}{'Real':>{ancho_num}}"
    filas_tabla = [header]

    tot_est = 0
    tot_real = 0
    claves_ordenadas = sorted(datos.keys(), key=lambda c: [str(x) for x in c])
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
        f"\n📦 Total general: estimado {formatear_kg(tot_est)} {unidad}, real {formatear_kg(tot_real)} {unidad}"
    )
    return "\n".join(lineas)

def obtener_cosecha_flexible(agrupar_por, fecha_inicio=None, fecha_fin=None, especie=None, variedad=None,
                              productor=None, packing=None, grupo=None, envase=None, temporada=None):
    """
    Consulta genérica: agrupa por las dimensiones exactas que se pidan (cualquier combinación
    de especie/variedad/productor/packing/grupo/fecha, o ninguna si se pide solo el total),
    sumando estimado (Trisemanal) y real (Recepción Planta) o Bultos si se da envase. A
    diferencia de las demás consultas, si no se da ningún periodo (ni fechas ni temporada) NO
    asume nada: pide que se aclare el periodo.
    """
    try:
        dimensiones = [d for d in (agrupar_por or []) if d in DIMENSIONES_SQL]

        if not fecha_inicio and not temporada:
            return "¿Para qué periodo necesitas este dato? (por ejemplo: esta temporada, un rango de fechas específico, o solo hoy)"

        conn = conectar_sql()
        if not conn:
            return "Error de conexión a base de datos"

        cursor = conn.cursor()

        if not fecha_inicio:
            cursor.execute(
                "SELECT MIN(CAST(Fecha AS DATE)), MAX(CAST(Fecha AS DATE)) FROM [Recepcion_Consolidada] WHERE Temporada = ?",
                (temporada,)
            )
            fila = cursor.fetchone()
            if not fila or not fila[0]:
                conn.close()
                return f"No encontré datos para la temporada {temporada}."
            fecha_inicio = fila[0].strftime("%Y-%m-%d")
            fecha_fin = fila[1].strftime("%Y-%m-%d")
        elif not fecha_fin:
            fecha_fin = fecha_inicio

        try:
            datetime.strptime(fecha_inicio, "%Y-%m-%d")
            datetime.strptime(fecha_fin, "%Y-%m-%d")
        except (ValueError, TypeError):
            conn.close()
            return "No entendí el rango de fechas. ¿Puedes indicarlo como 'entre el DD-MM-YYYY y el DD-MM-YYYY'?"

        condiciones = ["[Base Origen] IN (?, ?)", "CAST(Fecha AS DATE) BETWEEN ? AND ?"]
        params = [BASE_ORIGEN_ESTIMADO, BASE_ORIGEN_REAL, fecha_inicio, fecha_fin]
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

        resultado = formatear_cosecha_flexible(filas, dimensiones, fecha_inicio, fecha_fin, filtro_desc, unidad)
        if not resultado:
            return f"No hay datos registrados{filtro_desc} entre {fecha_inicio} y {fecha_fin}"
        return resultado
    except Exception as e:
        logger.error(f"Error en obtener_cosecha_flexible: {str(e)}")
        return f"Error al consultar: {str(e)}"

def _obtener_extremo_cosecha(especie, variedad, packing, productor, temporada, usar_maximo):
    """Función compartida: encuentra la primera (MIN) o última (MAX) fecha con cosecha real."""
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

def obtener_ultima_cosecha(especie=None, variedad=None, packing=None, productor=None, temporada=None):
    """Encuentra la última (más reciente) fecha con cosecha real, y muestra su detalle"""
    return _obtener_extremo_cosecha(especie, variedad, packing, productor, temporada, usar_maximo=True)

def obtener_primera_cosecha(especie=None, variedad=None, packing=None, productor=None, temporada=None):
    """
    Encuentra la primera fecha con cosecha real (cuándo empezó), y muestra su detalle.
    Si no se especifica temporada, se limita a la temporada vigente por defecto (a diferencia
    de "última cosecha", "primera cosecha" sin temporada casi siempre implica "esta temporada",
    no la primera vez registrada en toda la historia).
    """
    temporada = temporada or TEMPORADA_ACTUAL
    return _obtener_extremo_cosecha(especie, variedad, packing, productor, temporada, usar_maximo=False)

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
        "description": "Consulta el detalle de cosecha (estimado trisemanal y real), agrupado por packing/productor/variedad, con desglose por fecha si el rango no es muy amplio. Usar para preguntas con un periodo ACOTADO y explícito, como '¿qué se cosechó ayer?', '¿cuánto se cosechó entre el 1 y el 15 de agosto?', 'kilos recepcionados en tal planta esta semana', o cuando el usuario pidió explícitamente el 'detalle'/desglose por fecha, opcionalmente filtrado por especie, variedad, productor, packing/planta o grupo. NO uses esta herramienta como respuesta por defecto a una pregunta abierta tipo 'cosecha de esta temporada' o 'cosecha de tal variedad' sin periodo acotado ni estructura pedida — en esos casos primero hay que preguntarle al usuario cómo quiere el resumen (ver instrucciones generales).",
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
        "description": "Consulta GENÉRICA de estimado y real, agrupada EXACTAMENTE por las dimensiones que pida el usuario (cualquier combinación de especie, variedad, productor, packing, grupo, fecha — o ninguna si pide solo el total sin desglose). Usar cuando el usuario pide una estructura específica que no calza con las otras herramientas, por ejemplo: 'estimación de cosecha por especie' (agrupar_por=['especie']), 'informe con columnas fecha, estimado y real' (agrupar_por=['fecha']), 'total por productor' (agrupar_por=['productor']), 'solo el total' o 'cuánto es en total' (agrupar_por=[], sin desglose). Responde solo con las columnas pedidas, nada más. ESTA HERRAMIENTA NO TIENE PERIODO POR DEFECTO (a diferencia de las demás): si el usuario menciona CUALQUIER periodo, aunque sea 'esta temporada' o 'hasta hoy', DEBES pasar temporada o fecha_inicio/fecha_fin explícitamente — nunca los omitas solo porque suene al comportamiento por defecto de otras herramientas. Solo omite ambos si el usuario literalmente no dijo nada sobre tiempo, para que la herramienta pida aclaración.",
        "input_schema": {
            "type": "object",
            "properties": {
                "agrupar_por": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["especie", "variedad", "productor", "packing", "grupo", "fecha"]},
                    "description": "Dimensiones exactas por las que agrupar, en el orden pedido por el usuario. Ej. 'resumen por especie' -> ['especie']; 'informe con fecha, estimado y real' -> ['fecha']; 'total por productor y variedad' -> ['productor','variedad']; 'solo el total, sin desglose' -> [] (arreglo vacío)."
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
                }
            },
            "required": ["agrupar_por"]
        }
    },
]

def construir_system_prompt(es_audio=False):
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

    return f"""Eres el asistente de WhatsApp de Agua Santa para consultas de cosecha de fruta.
{nota_audio}

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

EXCEPCIÓN a lo anterior: consultar_cosecha_flexible NO tiene ningún periodo por defecto. Si vas a llamar
esa herramienta específicamente y el usuario dijo "esta temporada" (o cualquier referencia temporal),
DEBES pasar temporada={TEMPORADA_ACTUAL} explícitamente (o las fechas que correspondan) — no lo omitas
pensando que hay un default, porque en esa herramienta omitirlo significa "el usuario no dijo nada" y
hará que se le pregunte innecesariamente.

Tienes herramientas para consultar, por variedad: estimado de temporada (trisemanal), cosecha real en
una fecha, calibre promedio, y comparación de avance (estimado vs cosechado real) en una fecha.
También puedes consultar resúmenes por productor o por packing (no requieren variedad), cuándo fue la
primera o la última cosecha real de una especie/variedad/productor/packing (útil para "¿cuándo empezó
la cosecha?" o "¿cuándo fue la última?"), y el detalle de cosecha real entre un rango de fechas
(agrupado por especie/variedad/productor, opcionalmente filtrado por especie, variedad o productor).

Además tienes consultar_comparativo_estimaciones: compara Estimación Invierno vs Estimación Primavera
vs Cosecha Real, con diferencias y %, para preguntas tipo "comparativo de estimación invierno, primavera
y real" o "diferencia entre lo estimado en invierno y primavera". Estas son dos ciclos de estimación
distintos al "estimado" (Trisemanal) que usan las otras herramientas — úsala específicamente cuando el
usuario mencione "invierno" y/o "primavera" en el contexto de estimaciones.

También tienes consultar_cosecha_flexible: para cuando el usuario pide una estructura o agrupación
específica que no calza con las demás herramientas (ej. "estimación de cosecha por especie", "informe
con columnas fecha, estimado y real", "total por productor", "por grupo", o "solo el total sin
desglose" con agrupar_por=[]). Responde SOLO con lo que se pidió, ni más ni menos — si piden agrupar
solo por especie, no agregues variedad/productor/fecha aunque los tengas disponibles.

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
darte el total general, o desglosado por productor/fundo, por especie, por packing/planta, por grupo, o
el detalle completo día por día." Si tampoco quedó claro el periodo (recuerda que consultar_cosecha_flexible,
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

def ejecutar_tool(tool_name, tool_input):
    if tool_name == "consultar_resumen_productor":
        return obtener_resumen_por_productor(tool_input.get("productor", ""))
    if tool_name == "consultar_resumen_packing":
        return obtener_resumen_por_packing(tool_input.get("packing", ""))

    if tool_name == "consultar_ultima_cosecha":
        especie = tool_input.get("especie") or None
        variedad_op = normalizar_variedad(tool_input["variedad"]) if tool_input.get("variedad") else None
        return obtener_ultima_cosecha(
            especie=especie,
            variedad=variedad_op,
            packing=tool_input.get("packing") or None,
            productor=tool_input.get("productor") or None,
            temporada=tool_input.get("temporada") or None,
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

        response = claude_client.messages.create(
            model="claude-sonnet-5",
            max_tokens=300,
            system=construir_system_prompt(es_audio),
            tools=TOOLS,
            messages=messages,
        )

        tool_use_block = next((b for b in response.content if b.type == "tool_use"), None)
        if tool_use_block:
            tool_input = tool_use_block.input
            # Si el usuario pidió "detalle" explícitamente, forzamos el desglose por fecha
            # sin depender de que Claude lo haya marcado (instrucción "sí o sí").
            if tool_use_block.name == "consultar_cosecha_detalle" and "detalle" in texto_mensaje.lower():
                tool_input = {**tool_input, "detalle_por_fecha": True}
            return ejecutar_tool(tool_use_block.name, tool_input)

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
        else:
            logger.info(f"WhatsApp enviado a {numero_destino}: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error enviando WhatsApp: {str(e)}")
        return False

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
