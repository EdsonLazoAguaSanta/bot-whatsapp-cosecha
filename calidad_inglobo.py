"""Calidad de producto terminado desde Inglobo (EON Labs): el cliente de su API para el bot.

Lo que importa del manual de integración (pruebas en vivo del 16-11-2026):
- La API hoy responde SIN token y entonces mezcla datos de otras organizaciones. Este cliente nunca llama sin
  token: si no hay sesión válida, no hay consulta. La URL, el usuario y la clave van SOLO en el .env (el
  repositorio del bot es público y la API hoy está abierta).
- Siempre con fechas y día por día: con rangos la API corta sin avisar (2.000 evaluaciones, 100 planillas).
- El semáforo de la caja es el campo `nota` de la fila (VERDE / AMARILLO / ROJO), que ya trae la resolución de la
  contramuestra; `__colors` (1 a 4, ausente = en norma) solo dice QUÉ defecto falló. No se recalcula la nota.
- Para «muestras con problema» se mira la nota, no el filtro defectId (ese trae donde el defecto se EVALUÓ).
- Las fotos (`gallery`) traen URL absoluta: se mandan por WhatsApp con el link. Las de una caja salen de pedir el
  reporte con su sampleId (la galería del día no dice a qué caja pertenece cada foto).

Manual ampliado el 23-11-2026 (§6-B, §11 y §12) y decisiones de Edson del mismo día:
- Caché local de la temporada (inglobo_cache.db, junto a este archivo, fuera del repositorio): un reporte por día. Hoy y
  ayer se consultan siempre en vivo (el informe de las 08:00 no cambia); los días anteriores salen del caché, que
  sincronizar() rellena y re-baja cada noche (los últimos DIAS_REFRESCO días, porque Inglobo completa hacia atrás).
- Brix o Peso neto en 0 (o 0,01) es «sin medir / sin pesar», no un rechazo: no cuenta como defecto fuera de norma ni se
  promedia. La caja sigue con la nota que le puso Inglobo (si quedó roja solo por eso, se avisa).
- Promedio de Brix (promedio_brix), solo de las filas de medición (Muestra y Contramuestra, no las de Promedio).
"""
import base64
import io
import json
import logging
import os
import re
import sqlite3
import threading
import time
import unicodedata
import zlib
from collections import OrderedDict
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

TIMEOUT = 30            # el manual pide al menos 25 s: la primera consulta de un día tarda ~7 s
MAX_DIAS = 400          # rango máximo de una consulta: toda una temporada (se pide día por día, del caché)
TIPO_PLANILLA = "quality"   # «Producto terminado»
# Caché de la temporada (23-11-2026). Inglobo tiene datos desde el 23-06-2026 (temporada 2027).
CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inglobo_cache.db")
DIAS_EN_VIVO = 2         # hoy y ayer: siempre en vivo
DIAS_REFRESCO = 14       # la sincronización de cada noche vuelve a bajar estos últimos días
MAX_DIAS_SIN_CACHE = 10  # más días que esto sin guardar en un rango: se pide esperar (en vivo serían minutos)
# Columnas en que un valor <= UMBRAL_SIN_MEDIR es «sin medir» (Brix) o «sin pesar» (Peso neto), según Edson el 23-11.
SIN_MEDIR = {"BRIX", "PESO NETO (KG)"}
UMBRAL_SIN_MEDIR = 0.1
# Columnas numéricas que cajas() deja por fila de medición (para Brix promedio y Total calidad / condición %).
MEDIDAS_CAJA = ("BRIX", "TOTAL CALIDAD", "TOTAL CONDICION")
EMOJI_NOTA = {"VERDE": "🟢", "AMARILLO": "🟡", "ROJO": "🔴"}
EMOJI_CELDA = {2: "🟡", 3: "🟠", 4: "🔴"}
ORDEN_NOTA = {"ROJO": 0, "AMARILLO": 1, "": 2, "VERDE": 3}
TTL_HOY, TTL_PASADO = 120, 900   # segundos de caché: hoy cambia a cada rato, los días anteriores casi no


class ErrorCalidad(Exception):
    """Un problema con Inglobo que se le puede contar tal cual a quien consulta."""


def _env(nombre, defecto=""):
    return (os.getenv(nombre) or defecto).strip()


def _url():
    url = _env("INGLOBO_API_URL").rstrip("/")
    return url if url.startswith("https://") else ""


def configurada():
    return bool(_url() and _env("INGLOBO_USUARIO") and _env("INGLOBO_CLAVE"))


def _inicio_datos():
    """Primer día que se sincroniza (INGLOBO_DESDE, AAAA-MM-DD). Por defecto el 20-06-2026: Inglobo parte el 23-06."""
    try:
        return datetime.strptime(_env("INGLOBO_DESDE", "2026-06-20"), "%Y-%m-%d").date()
    except ValueError:
        return date(2026, 6, 20)


def _dominios_fotos():
    return [d.strip().lower() for d in _env("INGLOBO_DOMINIO_FOTOS", "digitaloceanspaces.com").split(",") if d.strip()]


def _norm(texto):
    sin_tildes = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode()
    return " ".join(sin_tildes.upper().split())


def _texto(valor):
    return "" if valor is None else str(valor).strip()


# ------------------------------------------------------------------------------------------------- sesión

_token = None
_candado = threading.Lock()


def _payload_jwt(token):
    try:
        parte = token.split(".")[1]
        parte += "=" * (-len(parte) % 4)
        return json.loads(base64.urlsafe_b64decode(parte))
    except Exception:
        return None


def _login():
    """Inicia sesión y devuelve el token (nunca se escribe en el log)."""
    global _token
    try:
        r = requests.post(f"{_url()}/auth/login", timeout=TIMEOUT,
                          json={"username": _env("INGLOBO_USUARIO"), "password": _env("INGLOBO_CLAVE")})
    except requests.RequestException as e:
        logger.error(f"Inglobo: no pude iniciar sesión ({type(e).__name__})")
        raise ErrorCalidad("No pude conectarme a Inglobo. Intenta de nuevo en un rato.")
    if r.status_code != 200:
        logger.error(f"Inglobo: el inicio de sesión respondió {r.status_code}")
        raise ErrorCalidad("Inglobo rechazó el inicio de sesión del bot: hay que revisar el usuario y la clave.")
    try:
        token = ((r.json() or {}).get("data") or {}).get("token")
    except ValueError:
        token = None
    datos = _payload_jwt(token) if isinstance(token, str) else None
    if not datos:
        raise ErrorCalidad("Inglobo no entregó un token válido: no consulto nada sin token.")
    esperada = _env("INGLOBO_ORGANIZACION")
    if esperada and str(datos.get("organizationId")) != esperada:
        logger.error(f"Inglobo: el token es de la organización {datos.get('organizationId')}, se esperaba {esperada}")
        raise ErrorCalidad("El usuario de Inglobo del bot es de otra organización: no consulto nada.")
    logger.info(f"Inglobo: sesión iniciada (organización {datos.get('organizationId')}, perfil {datos.get('role')})")
    _token = token
    return token


def _get(ruta, params):
    """GET con el token. Ante 401/403 vuelve a iniciar sesión una vez. Nunca llama sin token."""
    global _token
    if not configurada():
        raise ErrorCalidad("La consulta de calidad (Inglobo) todavía no está configurada en el bot.")
    for intento in (1, 2):
        with _candado:
            token = _token or _login()
        if not token:
            raise ErrorCalidad("Sin sesión en Inglobo: no consulto nada sin token.")
        inicio = time.monotonic()
        try:
            r = requests.get(f"{_url()}{ruta}", params=params, timeout=TIMEOUT,
                             headers={"Authorization": f"Bearer {token}"})
        except requests.Timeout:
            raise ErrorCalidad("Inglobo no respondió a tiempo. Intenta de nuevo en un rato.")
        except requests.RequestException as e:
            logger.error(f"Inglobo: error de conexión en {ruta} ({type(e).__name__})")
            raise ErrorCalidad("No pude conectarme a Inglobo. Intenta de nuevo en un rato.")
        if r.status_code in (401, 403) and intento == 1:
            with _candado:
                _token = None
            continue
        if r.status_code == 400:
            try:
                detalle = (r.json() or {}).get("message") or ""
            except ValueError:
                detalle = ""
            raise ErrorCalidad(f"Inglobo rechazó la consulta{': ' + str(detalle)[:200] if detalle else ''}.")
        if r.status_code != 200:
            logger.error(f"Inglobo: {ruta} respondió {r.status_code}")
            raise ErrorCalidad(f"Inglobo respondió con un error ({r.status_code}). Intenta de nuevo en un rato.")
        try:
            cuerpo = r.json() or {}
        except ValueError:
            raise ErrorCalidad("Inglobo respondió algo que no entiendo.")
        logger.info(f"Inglobo: {ruta} {params.get('startDate')}..{params.get('endDate')} en "
                    f"{time.monotonic() - inicio:.1f} s")
        return cuerpo.get("data") or {}
    raise ErrorCalidad("Inglobo no aceptó la sesión del bot: hay que revisar el usuario y la clave.")


_cache = {}
_candado_cache = threading.Lock()


def _cacheado(clave, ttl, funcion):
    ahora = time.monotonic()
    with _candado_cache:
        guardado = _cache.get(clave)
        if guardado and ahora - guardado[0] < ttl:
            return guardado[1]
    valor = funcion()
    with _candado_cache:
        _cache[clave] = (ahora, valor)
        for k in [k for k, (t, _) in _cache.items() if ahora - t > TTL_PASADO]:
            _cache.pop(k, None)
    return valor


def _params(dia, **extra):
    return {"startDate": dia.isoformat(), "endDate": dia.isoformat(), "templateType": TIPO_PLANILLA,
            **{k: v for k, v in extra.items() if v}}


def _del_cache(dia):
    """¿Ese día sale del caché local? Sí, salvo hoy y ayer (DIAS_EN_VIVO), que van siempre en vivo."""
    return dia <= date.today() - timedelta(days=DIAS_EN_VIVO)


def reporte_dia(dia, sample_id=None):
    """El reporte de un día (o de una caja, con sample_id). Los días anteriores a ayer salen del caché local (si no
    está, se baja y se guarda); hoy, ayer y las cajas puntuales, en vivo."""
    if sample_id is None and _del_cache(dia):
        return _dia_guardado(dia)
    ttl = TTL_HOY if dia >= date.today() else TTL_PASADO
    return _cacheado(("reporte", dia, sample_id), ttl,
                     lambda: _get("/reports/finished-product-verification", _params(dia, sampleId=sample_id)))


# ------------------------------------------------------------------------------------- caché local (23-11-2026)

_candado_db = threading.Lock()
_candado_sync = threading.Lock()
_EN_MEMORIA = OrderedDict()   # los últimos días leídos del caché, ya descomprimidos
MAX_EN_MEMORIA = 20


def _db():
    conn = sqlite3.connect(CACHE_PATH, timeout=30)
    conn.execute("CREATE TABLE IF NOT EXISTS dias (fecha TEXT PRIMARY KEY, datos BLOB, filas INTEGER, bajado TEXT)")
    return conn


def _guardar_dia(dia, data):
    blob = zlib.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"), 6)
    with _candado_db:
        conn = _db()
        try:
            conn.execute("INSERT OR REPLACE INTO dias (fecha, datos, filas, bajado) VALUES (?, ?, ?, ?)",
                         (dia.isoformat(), blob, len(data.get("matriz") or []), datetime.now().isoformat(timespec="seconds")))
            conn.commit()
        finally:
            conn.close()
        _EN_MEMORIA.pop(dia, None)


def _leer_dia(dia):
    with _candado_db:
        if dia in _EN_MEMORIA:
            _EN_MEMORIA.move_to_end(dia)
            return _EN_MEMORIA[dia]
        conn = _db()
        try:
            fila = conn.execute("SELECT datos FROM dias WHERE fecha = ?", (dia.isoformat(),)).fetchone()
        finally:
            conn.close()
        if not fila:
            return None
        data = json.loads(zlib.decompress(fila[0]).decode("utf-8"))
        _EN_MEMORIA[dia] = data
        while len(_EN_MEMORIA) > MAX_EN_MEMORIA:
            _EN_MEMORIA.popitem(last=False)
        return data


def _dia_guardado(dia):
    data = _leer_dia(dia)
    if data is None:
        data = _get("/reports/finished-product-verification", _params(dia))
        _guardar_dia(dia, data)
    return data


def dias_guardados():
    """{AAAA-MM-DD: hora en que se bajó} de los días que ya están en el caché local."""
    with _candado_db:
        conn = _db()
        try:
            return dict(conn.execute("SELECT fecha, bajado FROM dias").fetchall())
        finally:
            conn.close()


def sincronizar(completo=False):
    """Tarea de cada noche (y al iniciar el bot): baja a inglobo_cache.db los días que faltan desde _inicio_datos()
    hasta anteayer y vuelve a bajar los últimos DIAS_REFRESCO (Inglobo completa hacia atrás las muestras sin medir).
    completo=True vuelve a bajar todo. Se detiene si Inglobo falla 3 veces seguidas. Devuelve un resumen."""
    if not configurada():
        return {"estado": "Inglobo no está configurado"}
    if not _candado_sync.acquire(blocking=False):
        return {"estado": "ya hay una sincronización en curso"}
    try:
        inicio = time.monotonic()
        hasta = date.today() - timedelta(days=DIAS_EN_VIVO)
        limite = hasta - timedelta(days=DIAS_REFRESCO - 1)
        ya = dias_guardados()
        bajados, filas, fallas_seguidas, errores = 0, 0, 0, 0
        dia = _inicio_datos()
        while dia <= hasta:
            if completo or dia >= limite or dia.isoformat() not in ya:
                try:
                    data = _get("/reports/finished-product-verification", _params(dia))
                    _guardar_dia(dia, data)
                    bajados += 1
                    filas += len(data.get("matriz") or [])
                    fallas_seguidas = 0
                except ErrorCalidad as e:
                    errores += 1
                    fallas_seguidas += 1
                    logger.warning(f"Inglobo: no pude sincronizar el {dia:%d-%m-%Y} ({e})")
                    if fallas_seguidas >= 3:
                        logger.error("Inglobo: 3 fallas seguidas, la sincronización se detiene hasta la próxima")
                        break
            dia += timedelta(days=1)
        resumen = {"estado": "ok" if not errores else "con errores", "dias_bajados": bajados, "filas": filas,
                   "errores": errores, "dias_guardados": len(dias_guardados()),
                   "segundos": round(time.monotonic() - inicio, 1)}
        logger.info(f"Inglobo: sincronización {resumen}")
        return resumen
    finally:
        _candado_sync.release()


def filtros_dia(dia):
    ttl = TTL_HOY if dia >= date.today() else TTL_PASADO
    return _cacheado(("filtros", dia), ttl,
                     lambda: (_get("/reports/finished-product-verification/filters", _params(dia)) or {}).get("options")
                     or {})


# ------------------------------------------------------------------------------------------------- fechas

def leer_fecha(texto, defecto=None):
    """AAAA-MM-DD o DD-MM-AAAA (también con /). Sin texto, el defecto (hoy)."""
    if isinstance(texto, date):
        return texto
    t = _texto(texto)
    if not t:
        return defecto or date.today()
    for formato in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(t, formato).date()
        except ValueError:
            pass
    raise ErrorCalidad(f"No entendí la fecha «{t}»: usa AAAA-MM-DD.")


def dias_entre(desde, hasta):
    """Los días del rango que pueden tener datos (desde _inicio_datos() hasta hoy; [] si no queda ninguno). Si faltan
    muchos en el caché local (la primera carga de la temporada aún no termina), pide esperar: en vivo serían minutos."""
    if hasta < desde:
        desde, hasta = hasta, desde
    if (hasta - desde).days + 1 > MAX_DIAS:
        raise ErrorCalidad(f"Consulto hasta {MAX_DIAS} días seguidos. Pide un rango más corto.")
    desde, hasta = max(desde, _inicio_datos()), min(hasta, date.today())
    if hasta < desde:
        return []
    dias = [desde + timedelta(days=i) for i in range((hasta - desde).days + 1)]
    guardados = dias_guardados()
    faltan = [d for d in dias if _del_cache(d) and d.isoformat() not in guardados]
    if len(faltan) > MAX_DIAS_SIN_CACHE:
        raise ErrorCalidad(f"Todavía no tengo guardados {len(faltan)} de esos días de Inglobo (el bot está cargando la "
                           "temporada). Pide un rango más corto o intenta de nuevo en unos minutos.")
    return dias


# ------------------------------------------------------------------------------------------------- cajas

# Columnas de metadata que se buscan por su key o por su encabezado (sin tildes ni mayúsculas).
CAMPOS = {
    "folio": ("folio", "numeroProceso", "Folio", "N° proceso"),
    "fecha": ("fecha", "Fecha"),
    "hora": ("hora", "Hora"),
    "packing": ("packing", "Packing", "Planta"),
    "productor": ("productor", "Productor"),
    # El mismo código de productor que Control_EAS (dbo.Expo-Growers), visto el 17-11-2026.
    "codigo_productor": ("codigoProductor", "Cód. Productor"),
    "especie": ("especie", "Especie"),
    "variedad": ("variedad", "Variedad"),
    "calibre": ("calibre", "Calibre"),
    "embalaje": ("embalaje", "Embalaje"),
    "turno": ("turno", "Turno"),
    "resolucion": ("resolucion", "Resolución"),
}


def _meta(fila, cols, nombres):
    for n in nombres:
        if _texto(fila.get(n)):
            return _texto(fila.get(n))
    buscados = {_norm(n) for n in nombres}
    for c in cols:
        if c.get("kind") == "meta" and (_norm(c.get("header")) in buscados or _norm(c.get("key")) in buscados):
            if _texto(fila.get(c.get("key"))):
                return _texto(fila.get(c.get("key")))
    return ""


def _color(fila, key):
    try:
        return int(((fila.get("__colors") or {}).get(key)) or 1)
    except (TypeError, ValueError):
        return 1


def _valor(v):
    if isinstance(v, float):
        return f"{v:.2f}".rstrip("0").rstrip(".").replace(".", ",")
    return _texto(v)


def _sin_medir(col, valor):
    """Brix o Peso neto en 0 (o 0,01): «sin medir / sin pesar», no un valor real (Edson, 23-11-2026)."""
    return (_norm(col.get("header")) in SIN_MEDIR and isinstance(valor, (int, float)) and not isinstance(valor, bool)
            and valor <= UMBRAL_SIN_MEDIR)


def _es_promedio(fila):
    return bool(fila.get("__average")) or _texto(fila.get("tipo")).lower().startswith("promedio")


def _es_contramuestra(fila):
    return _texto(fila.get("tipo")).lower().startswith("contra")


def cajas(data):
    """Las cajas del reporte: cada una con su fila de muestra, sus contramuestras y su promedio. Las filas vienen
    agrupadas (Muestra → Contramuestra(s) → Promedio): cada «Muestra» abre una caja."""
    cols = data.get("matrizCols") or []
    defectos = [c for c in cols if c.get("kind") == "defect"]
    grupos, actual = [], None
    for fila in data.get("matriz") or []:
        if not isinstance(fila, dict):
            continue
        if actual is None or not (_es_promedio(fila) or _es_contramuestra(fila)):
            actual = []
            grupos.append(actual)
        actual.append(fila)
    # Las filas no traen la especie: sale del resumen `muestras` (o de la galería), por variedad.
    especie_de = {}
    for m in (data.get("muestras") or []) + (data.get("gallery") or []):
        if isinstance(m, dict) and _texto(m.get("variedad")) and _texto(m.get("especie")):
            especie_de.setdefault(_norm(m.get("variedad")), _texto(m.get("especie")))
    resultado = []
    for filas in grupos:
        muestra = next((f for f in filas if not (_es_promedio(f) or _es_contramuestra(f))), filas[0])
        promedio = next((f for f in filas if _es_promedio(f)), None)
        principal = promedio or muestra
        nota = (_texto(principal.get("nota")) or _texto(muestra.get("nota"))).upper()

        def fuera_de(fila):
            # Lo «sin medir» (Brix o Peso neto en 0) no es un defecto fuera de norma, aunque Inglobo lo pinte rojo.
            return [(c.get("header") or c.get("key"), fila.get(c.get("key")), _color(fila, c.get("key")))
                    for c in defectos if _color(fila, c.get("key")) >= 2 and not _sin_medir(c, fila.get(c.get("key")))]
        fuera = fuera_de(principal)
        if not fuera and nota != "VERDE":
            fuera = fuera_de(muestra)
        sin_medir = list(dict.fromkeys(c.get("header") or c.get("key") for f in filas for c in defectos
                                       if _color(f, c.get("key")) >= 2 and _sin_medir(c, f.get(c.get("key")))))
        caja = {k: _meta(muestra, cols, v) for k, v in CAMPOS.items()}
        caja["especie"] = caja["especie"] or especie_de.get(_norm(caja["variedad"]), "")
        # Mediciones de cada fila de muestra y contramuestra (sin la de Promedio) para los promedios del informe.
        medidas = {}
        for nombre in MEDIDAS_CAJA:
            claves = [c.get("key") for c in defectos if _norm(c.get("header")) == nombre]
            if claves:
                medidas[nombre] = [max((f.get(k) for k in claves if isinstance(f.get(k), (int, float))
                                        and not isinstance(f.get(k), bool)), default=None)
                                   for f in filas if not _es_promedio(f)]
        caja.update(nota=nota, fuera=sorted(fuera, key=lambda x: -x[2]), filas=filas, medidas=medidas,
                    muestras=sum(1 for f in filas if not _es_promedio(f)),
                    contramuestras=sum(1 for f in filas if _es_contramuestra(f)), sin_medir=sin_medir,
                    # Roja (así la marca Inglobo) sin ningún defecto fuera de norma: solo por algo sin medir.
                    solo_sin_medir=nota == "ROJO" and not fuera and bool(sin_medir))
        resultado.append(caja)
    return resultado


def _linea_caja(c):
    datos = [f"Folio {c['folio'] or '?'}", " ".join(x for x in (c["fecha"][:10], c["hora"]) if x),
             c["packing"], c["productor"], " ".join(x for x in (c["especie"].capitalize(), c["variedad"]) if x),
             f"cal. {c['calibre']}" if c["calibre"] else "",
             (c["turno"] if _norm(c["turno"]).startswith("TURNO") else f"turno {c['turno']}") if c["turno"] else ""]
    texto = f"{EMOJI_NOTA.get(c['nota'], '⚪')} " + " · ".join(d for d in datos if d)
    if c["fuera"]:
        texto += " — " + "; ".join(f"{h}: {_valor(v)} {EMOJI_CELDA.get(col, '')}".rstrip() for h, v, col in c["fuera"][:6])
        if len(c["fuera"]) > 6:
            texto += f" (+{len(c['fuera']) - 6} más)"
    if c.get("sin_medir"):
        texto += f" · sin medir (en 0): {', '.join(c['sin_medir'])}"
    if c["resolucion"] and c["resolucion"] not in ("—", "-"):
        texto += f" · Resolución: {c['resolucion']}"
    return texto


def _conteo(lista):
    n = {k: 0 for k in ("VERDE", "AMARILLO", "ROJO", "")}
    for c in lista:
        n[c["nota"] if c["nota"] in n else ""] += 1
    partes = [f"{EMOJI_NOTA[k]} {n[k]}" for k in ("VERDE", "AMARILLO", "ROJO")]
    if n[""]:
        partes.append(f"sin nota {n['']}")
    return f"{len(lista)} cajas: " + " · ".join(partes)


def _coincide(buscado, valor):
    b, v = _norm(buscado), _norm(valor)
    return bool(b) and b in v


def resumen(desde, hasta, permitido, planta=None, productor=None, variedad=None, especie=None, turno=None,
            solo_problemas=True, max_cajas=40):
    """Texto para Claude: semáforo del período, por planta y productor, y las cajas con problema.
    permitido(caja) -> bool: el alcance de quien consulta (lo decide el bot, no lo que pida el modelo)."""
    todas = []
    for dia in dias_entre(desde, hasta):
        todas += cajas(reporte_dia(dia))
    vistas = [c for c in todas if permitido(c)]
    filtros = {"planta": (planta, "packing"), "productor": (productor, "productor"),
               "variedad": (variedad, "variedad"), "especie": (especie, "especie"), "turno": (turno, "turno")}
    for nombre, (buscado, campo) in filtros.items():
        if _texto(buscado):
            vistas = [c for c in vistas if _coincide(buscado, c[campo])]
    periodo = desde.strftime("%d-%m-%Y") + ("" if hasta == desde else f" al {hasta:%d-%m-%Y}")
    if not vistas:
        hay_otras = bool(todas) and not any(permitido(c) for c in todas)
        return (f"Inglobo no tiene cajas evaluadas (producto terminado) para {periodo} con esos filtros."
                + (" Hay cajas de otras plantas o productores, que esta persona no puede ver." if hay_otras else ""))
    lineas = [f"Calidad de producto terminado (Inglobo) · {periodo}", _conteo(vistas)]
    aprobadas = sum(1 for c in vistas if c["nota"] == "VERDE" and "contramuestra" in _norm(c["resolucion"]).lower())
    if aprobadas:
        lineas.append(f"De las 🟢, {aprobadas} quedaron aprobadas por contramuestra (la muestra había salido fuera "
                      "de norma; ya están contadas en las verdes)")
    solo_sin_medir = sum(1 for c in vistas if c.get("solo_sin_medir"))
    if solo_sin_medir:
        lineas.append(f"De las 🔴, {solo_sin_medir} {'lo son' if solo_sin_medir > 1 else 'lo es'} solo por Brix o "
                      "Peso neto en 0, que en Inglobo es «sin medir» y no un rechazo por ese defecto; se cuentan como "
                      "rojas porque así las marca Inglobo")
    for campo, titulo in (("packing", "Por planta"), ("productor", "Por productor")):
        grupos = {}
        for c in vistas:
            grupos.setdefault(c[campo] or "(sin dato)", []).append(c)
        if len(grupos) > 1 or campo == "packing":
            orden = sorted(grupos.items(), key=lambda g: (-sum(c["nota"] != "VERDE" for c in g[1]), -len(g[1])))
            if campo == "productor" and len(orden) > 15:
                orden = [g for g in orden if any(c["nota"] != "VERDE" for c in g[1])][:15]
            lineas.append(f"{titulo}:")
            lineas += [f"- {nombre}: {_conteo(lista)}" for nombre, lista in orden]
    problemas = sorted((c for c in vistas if c["nota"] != "VERDE"),
                       key=lambda c: (ORDEN_NOTA.get(c["nota"], 2), c["fecha"], c["hora"]))
    elegidas = problemas if solo_problemas else sorted(vistas, key=lambda c: (ORDEN_NOTA.get(c["nota"], 2),
                                                                                c["fecha"], c["hora"]))
    if elegidas:
        lineas.append("Cajas con amarillo o rojo:" if solo_problemas else "Cajas:")
        lineas += [_linea_caja(c) for c in elegidas[:max_cajas]]
        if len(elegidas) > max_cajas:
            lineas.append(f"(+{len(elegidas) - max_cajas} cajas más; acota por planta, productor o turno)")
    else:
        lineas.append("Ninguna caja con amarillo o rojo.")
    lineas.append("Semáforo: nota de la caja ya con la resolución de la contramuestra. Junto a cada defecto: "
                  "🟡 advertencia, 🟠 rechazo aprobado, 🔴 rechazado. Para el detalle y las fotos de una caja: "
                  "detalle_caja_calidad con su folio y fecha.")
    return "\n".join(lineas)


# ------------------------------------------------------------------------------------- Brix (§6-B, 23-11-2026)

AGRUPAR_BRIX = {"variedad": "variedad", "productor": "productor", "planta": "packing", "turno": "turno",
                "especie": "especie", "calibre": "calibre", "dia": "dia", "total": None}


def _decimal(x):
    return f"{x:.1f}".replace(".", ",")


def promedio_brix(desde, hasta, permitido, agrupar="variedad", planta=None, productor=None, variedad=None,
                  especie=None, turno=None, calibre=None):
    """Texto para Claude: el Brix promedio del período por grupo. Se promedian las mediciones de Muestra y
    Contramuestra (no las filas Promedio de Inglobo, que contarían doble) y se excluyen las «sin medir» (Brix 0 o
    0,01), que se informan aparte. permitido(caja): el alcance de quien consulta."""
    agrupar = _norm(agrupar).lower() if _texto(agrupar) else "variedad"
    agrupar = agrupar if agrupar in AGRUPAR_BRIX else "variedad"
    campo = AGRUPAR_BRIX[agrupar]
    dias = dias_entre(desde, hasta)
    filtros = {"packing": planta, "productor": productor, "variedad": variedad, "especie": especie, "turno": turno}
    grupos, cajas_vistas, hay_otras = {}, 0, False
    for dia in dias:
        data = reporte_dia(dia)
        claves = [c.get("key") for c in data.get("matrizCols") or []
                  if c.get("kind") == "defect" and _norm(c.get("header")) == "BRIX"]
        for caja in cajas(data):
            if not permitido(caja):
                hay_otras = True
                continue
            if any(_texto(b) and not _coincide(b, caja[k]) for k, b in filtros.items()):
                continue
            if _texto(calibre) and _norm(calibre) != _norm(caja["calibre"]):
                continue
            cajas_vistas += 1
            nombre = dia.isoformat() if campo == "dia" else "Total" if campo is None else (caja[campo] or "(sin dato)")
            g = grupos.setdefault(nombre, {"suma": 0.0, "n": 0, "sin_medir": 0})
            for fila in caja["filas"]:
                if _es_promedio(fila):
                    continue
                valores = [fila.get(k) for k in claves
                           if isinstance(fila.get(k), (int, float)) and not isinstance(fila.get(k), bool)]
                if not valores or max(valores) <= UMBRAL_SIN_MEDIR:
                    g["sin_medir"] += 1
                    continue
                g["suma"] += max(valores)
                g["n"] += 1
    periodo = (dias[0].strftime("%d-%m-%Y") + ("" if dias[0] == dias[-1] else f" al {dias[-1]:%d-%m-%Y}")) if dias else ""
    filtro = ", ".join(f"{k if k != 'packing' else 'planta'} {v}" for k, v in {**filtros, "calibre": calibre}.items()
                       if _texto(v))
    if not cajas_vistas:
        return (f"Inglobo no tiene cajas evaluadas (producto terminado) {('para ' + periodo) if periodo else 'en ese período'}"
                + (f" con {filtro}" if filtro else "") + "."
                + (" Hay cajas de otras plantas o productores, que esta persona no puede ver." if hay_otras else ""))
    n_total = sum(g["n"] for g in grupos.values())
    sin_medir = sum(g["sin_medir"] for g in grupos.values())
    titulo = (f"Brix promedio (Inglobo, producto terminado) · {periodo}" + (f" · {filtro}" if filtro else "")
              + (f" · por {agrupar}" if campo else ""))
    lineas = [titulo]
    orden = sorted(grupos.items(), key=lambda g: (g[0] if campo == "dia" else "", -(g[1]["suma"] / g[1]["n"]) if g[1]["n"] else 1))
    for nombre, g in orden:
        if campo == "dia":
            nombre = f"{date.fromisoformat(nombre):%d-%m-%Y}"
        lineas.append(f"- {nombre}: " + (f"{_decimal(g['suma'] / g['n'])}° ({g['n']} mediciones)" if g["n"]
                                         else "sin mediciones válidas")
                      + (f", {g['sin_medir']} sin medir" if g["sin_medir"] else ""))
    if campo and n_total:
        lineas.append(f"Total: {_decimal(sum(g['suma'] for g in grupos.values()) / n_total)}° ({n_total} mediciones de "
                      f"{cajas_vistas} cajas)")
    elif not n_total:
        lineas.append(f"Ninguna medición de Brix válida en {cajas_vistas} cajas.")
    lineas.append(f"{sin_medir} mediciones sin medir (Brix 0 o 0,01 en Inglobo) quedaron fuera del promedio."
                  if sin_medir else "Ninguna medición quedó fuera: no hubo Brix sin medir (0 o 0,01) en estas cajas.")
    lineas.append("Se promedian las mediciones de muestra y contramuestra (no las filas Promedio de Inglobo). Di cuántas "
                  "mediciones entraron y cuántas se excluyeron.")
    return "\n".join(lineas)


# ------------------------------------------------------------------------------------------------- una caja

def _folio_de_etiqueta(etiqueta):
    m = re.match(r"\s*Folio\s+([^\s·]+)", _texto(etiqueta), re.IGNORECASE)
    return m.group(1) if m else ""


def _mismo_folio(a, b):
    a, b = _texto(a).lstrip("0") or "0", _texto(b).lstrip("0") or "0"
    return a.upper() == b.upper()


def _foto_valida(url):
    try:
        partes = urlparse(_texto(url))
    except ValueError:
        return False
    host = (partes.hostname or "").lower()
    return partes.scheme == "https" and any(host == d or host.endswith("." + d) for d in _dominios_fotos())


def _es_rojo(color):
    """El color del rango de la foto: Inglobo marca en rojo (#FF0000) lo fuera de tolerancia y en verde lo demás."""
    m = re.fullmatch(r"#?([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})", _texto(color))
    return bool(m) and int(m.group(1), 16) >= 200 and int(m.group(2), 16) < 120


def _gravedad_foto(foto, fuera):
    """Orden de las fotos: primero las de los defectos que dejaron la caja fuera de norma (el más grave antes), después
    las que Inglobo marca fuera de tolerancia y al final el resto."""
    defecto = _norm(foto.get("defecto"))
    if defecto in fuera:
        return (0, -fuera[defecto])
    if _es_rojo(foto.get("rangoColor")) or "RECHAZ" in _norm(foto.get("rangoNombre")):
        return (1, 0)
    return (2, 0)


def _misma_hora(buscada, hora):
    """'8:59', '08:59' o '08' contra la hora de la caja."""
    b, h = _texto(buscada).replace(".", ":"), _texto(hora)
    if not b:
        return True
    if ":" in b:
        hh, mm = (b.split(":", 1) + ["00"])[:2]
        return hh.isdigit() and h[:5] == f"{int(hh):02d}:{mm[:2].zfill(2)}"
    return b.isdigit() and h[:2] == f"{int(b):02d}"


def detalle_caja(dia, folio, permitido, planta=None, productor=None, hora=None, calibre=None, max_fotos=4,
                 todas_las_fotos=False):
    """(texto para Claude, fotos [{url, caption}]) de UNA caja. El folio es el N° de proceso: un mismo folio suele
    tener varias cajas (muestras) en el día, que se distinguen por hora y calibre; si queda más de una, se listan.
    Fotos: por defecto solo las de los defectos fuera de norma (si no hay ninguna, las demás); todas_las_fotos=True
    agrega las de los atributos en norma."""
    del_folio = [c for c in cajas(reporte_dia(dia)) if _mismo_folio(c["folio"], folio)]
    if not del_folio:
        return f"No encontré el folio {folio} en Inglobo el {dia:%d-%m-%Y} (producto terminado).", []
    candidatas = [c for c in del_folio if permitido(c)]
    if not candidatas:
        return f"El folio {folio} del {dia:%d-%m-%Y} es de una planta o productor que esta persona no puede ver.", []
    for buscado, campo in ((planta, "packing"), (productor, "productor")):
        if _texto(buscado):
            candidatas = [c for c in candidatas if _coincide(buscado, c[campo])] or candidatas
    if _texto(hora):
        candidatas = [c for c in candidatas if _misma_hora(hora, c["hora"])] or candidatas
    if _texto(calibre):
        candidatas = [c for c in candidatas if _norm(calibre) == _norm(c["calibre"])] or candidatas
    if len(candidatas) > 1:
        return (f"El folio {folio} tiene {len(candidatas)} cajas evaluadas ese día (el folio es el N° de proceso: cada "
                "una es una muestra distinta). Pregunta cuál, por hora o calibre, y vuelve a llamar con ese dato:\n"
                + "\n".join(f"- {_linea_caja(c)}" for c in candidatas)), []
    caja = candidatas[0]
    data = _reporte_de_caja(dia, caja)
    cols = data.get("matrizCols") or reporte_dia(dia).get("matrizCols") or []
    lineas = [_linea_caja(caja)]
    meta = [c for c in cols if c.get("kind") == "meta"]
    muestra = caja["filas"][0]
    datos = [f"{c.get('header') or c.get('key')}: {_texto(muestra.get(c.get('key')))}"
             for c in meta if _texto(muestra.get(c.get("key"))) and not str(c.get("key", "")).startswith("__")]
    if datos:
        lineas.append("Datos: " + "; ".join(datos))
    defectos = [c for c in cols if c.get("kind") == "defect"]
    for fila in caja["filas"]:
        tipo = _texto(fila.get("tipo")) or "Muestra"
        fuera = [f"{c.get('header')}: {_valor(fila.get(c.get('key')))} "
                 + ("(sin medir)" if _sin_medir(c, fila.get(c.get("key"))) else EMOJI_CELDA[_color(fila, c.get("key"))])
                 for c in defectos if _color(fila, c.get("key")) >= 2]
        nota = _texto(fila.get("nota")).upper()
        lineas.append(f"{tipo} {EMOJI_NOTA.get(nota, '⚪')} {nota or 'sin nota'}: "
                      + ("; ".join(fuera) if fuera else "sin defectos fuera de norma"))
    principal = next((f for f in caja["filas"] if _es_promedio(f)), muestra)
    medidas = [f"{c.get('header')}: {_valor(principal.get(c.get('key')))}" for c in defectos
               if _texto(principal.get(c.get("key"))) not in ("", "0", "0.0")]
    if medidas:
        lineas.append(f"Todas las mediciones ({_texto(principal.get('tipo')) or 'muestra'}): " + "; ".join(medidas))
    if not data:
        lineas.append("No encontré las fotos de esta caja en Inglobo.")
        return "\n".join(lineas), []
    # Fotos: primero las de los defectos que la dejaron fuera de norma.
    galeria, graves, fuera = _fotos_ordenadas(caja, data)
    elegidas = galeria if todas_las_fotos or not graves else graves
    fotos = []
    for g in elegidas[:max(0, max_fotos)]:
        marca = EMOJI_CELDA.get(fuera.get(_norm(g.get("defecto"))), "🔴" if _es_rojo(g.get("rangoColor")) else "")
        partes = [f"Folio {caja['folio'] or folio} · {caja['hora']}".strip(" ·"),
                  f"{_texto(g.get('defecto'))}: {_valor(g.get('valor'))} {marca}".strip(": "),
                  _texto(g.get("observacion"))]
        fotos.append({"url": g["imageUrl"], "caption": " · ".join(p for p in partes if p)[:1000]})
    descartadas = len(data.get("gallery") or []) - len(galeria)
    if descartadas:
        # Fotos sin URL https del dominio de Inglobo (INGLOBO_DOMINIO_FOTOS): no se mandan ni se cuentan.
        logger.warning(f"Inglobo: {descartadas} foto(s) del folio {folio} con una URL fuera del dominio esperado")
    lineas.append(f"Fotos de esta caja en Inglobo: {len(galeria)}, {len(graves)} de defectos fuera de norma"
                  + (f"; se mandan {len(fotos)}" if fotos else "")
                  + (". Las demás son de atributos en norma: se mandan solo si las pide (todas_las_fotos)"
                     if graves and not todas_las_fotos and len(galeria) > len(graves) else ""))
    return "\n".join(lineas), fotos


# ------------------------------------------------------------------------------------- informe diario (18-11-2026)

def _reporte_de_caja(dia, caja):
    """El reporte propio de una caja (por su sampleId), que trae sus fotos. Un folio tiene varias cajas con la misma
    etiqueta, así que se busca entre las del folio la que calza en hora, calibre, productor y packing. {} si no está."""
    for o in filtros_dia(dia).get("sampleId") or []:
        if not (isinstance(o, dict) and _texto(o.get("value"))
                and _mismo_folio(_folio_de_etiqueta(o.get("label")), caja["folio"])):
            continue
        propio = reporte_dia(dia, sample_id=_texto(o.get("value")))
        if any((c["hora"], c["calibre"], c["productor"], c["packing"])
               == (caja["hora"], caja["calibre"], caja["productor"], caja["packing"]) for c in cajas(propio)):
            return propio
    return {}


def _fotos_ordenadas(caja, data):
    """(galería válida ordenada, las de defectos fuera de norma, {defecto: color}) de una caja."""
    defectos = [c for c in data.get("matrizCols") or [] if c.get("kind") == "defect"]
    fuera = {}
    for f in caja["filas"]:
        for c in defectos:
            if _color(f, c.get("key")) >= 2 and not _sin_medir(c, f.get(c.get("key"))):
                clave = _norm(c.get("header"))
                fuera[clave] = max(fuera.get(clave, 0), _color(f, c.get("key")))
    galeria = [g for g in data.get("gallery") or [] if isinstance(g, dict) and g.get("isImage", True)
               and _foto_valida(g.get("imageUrl"))]
    galeria.sort(key=lambda g: (_gravedad_foto(g, fuera), _texto(g.get("defecto"))))
    return galeria, [g for g in galeria if _gravedad_foto(g, fuera)[0] < 2], fuera


def defectos_rechazo(caja):
    """Los defectos que dejaron la caja fuera de norma [(defecto, valor, color)], sin los totales ni las notas
    (Total Calidad, Nota condición...) salvo que sean lo único marcado."""
    propios = [d for d in caja["fuera"] if not _norm(d[0]).startswith(("TOTAL ", "NOTA "))]
    return propios or caja["fuera"]


def foto_principal(dia, caja):
    """La foto del defecto que rechazó la caja: {url, defecto, valor} o None. Sale de la galería del día (ya está en
    caché): la foto de ese folio, packing y calibre, de ese defecto y con el mismo valor que midió la caja. Así no
    hay que pedir el reporte de cada caja (el informe diario tardaba ~1 minuto en eso)."""
    data = reporte_dia(dia)
    clave_de = {_norm(c.get("header")): c.get("key") for c in data.get("matrizCols") or [] if c.get("kind") == "defect"}
    galeria = [g for g in data.get("gallery") or [] if isinstance(g, dict) and g.get("isImage", True)
               and _foto_valida(g.get("imageUrl")) and _mismo_folio(g.get("numeroProceso"), caja["folio"])
               and _norm(g.get("packing")) == _norm(caja["packing"])
               and (not _texto(g.get("calibre")) or _norm(g.get("calibre")) == _norm(caja["calibre"]))]
    for defecto, _, _ in sorted(defectos_rechazo(caja), key=lambda d: -d[2]):
        medidos = {_valor(f.get(clave_de.get(_norm(defecto)))) for f in caja["filas"]} - {""}
        for g in galeria:
            if _norm(g.get("defecto")) == _norm(defecto) and _valor(g.get("valor")) in medidos:
                return {"url": g["imageUrl"], "defecto": _texto(g.get("defecto")), "valor": _valor(g.get("valor"))}
    return None


_MINIATURAS = {}
MAX_FOTO_BYTES = 8 * 1024 * 1024


def miniatura(url, lado=640):
    """La foto achicada (JPEG, lado máximo en píxeles) para el PDF, o None. Solo del dominio de Inglobo; sin token
    (las fotos se descargan directo de su almacenamiento)."""
    if not _foto_valida(url):
        return None
    clave = (url, lado)
    if clave in _MINIATURAS:
        return _MINIATURAS[clave]
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200 or len(r.content) > MAX_FOTO_BYTES:
            logger.warning(f"Inglobo: no pude bajar una foto ({r.status_code}, {len(r.content)} bytes)")
            return None
        from PIL import Image, ImageOps
        imagen = ImageOps.exif_transpose(Image.open(io.BytesIO(r.content))).convert("RGB")
        imagen.thumbnail((lado, lado))
        salida = io.BytesIO()
        imagen.save(salida, "JPEG", quality=72, optimize=True)
        contenido = salida.getvalue()
    except Exception as e:
        logger.warning(f"Inglobo: no pude preparar una foto ({type(e).__name__})")
        return None
    if len(_MINIATURAS) > 300:
        _MINIATURAS.clear()
    _MINIATURAS[clave] = contenido
    return contenido
