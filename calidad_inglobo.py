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
"""
import base64
import io
import json
import logging
import os
import re
import threading
import time
import unicodedata
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

TIMEOUT = 30            # el manual pide al menos 25 s: la primera consulta de un día tarda ~7 s
MAX_DIAS = 7            # rango máximo de una consulta (se pide día por día)
TIPO_PLANILLA = "quality"   # «Producto terminado»
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


def reporte_dia(dia, sample_id=None):
    """El reporte de un día (o de una caja, con sample_id)."""
    ttl = TTL_HOY if dia >= date.today() else TTL_PASADO
    return _cacheado(("reporte", dia, sample_id), ttl,
                     lambda: _get("/reports/finished-product-verification", _params(dia, sampleId=sample_id)))


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
    if hasta < desde:
        desde, hasta = hasta, desde
    if (hasta - desde).days + 1 > MAX_DIAS:
        raise ErrorCalidad(f"Consulto hasta {MAX_DIAS} días seguidos (Inglobo corta sin avisar los rangos largos). "
                           "Pide un rango más corto.")
    return [desde + timedelta(days=i) for i in range((hasta - desde).days + 1)]


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
        fuera = [(c.get("header") or c.get("key"), principal.get(c.get("key")), _color(principal, c.get("key")))
                 for c in defectos if _color(principal, c.get("key")) >= 2]
        if not fuera and nota != "VERDE":
            fuera = [(c.get("header") or c.get("key"), muestra.get(c.get("key")), _color(muestra, c.get("key")))
                     for c in defectos if _color(muestra, c.get("key")) >= 2]
        caja = {k: _meta(muestra, cols, v) for k, v in CAMPOS.items()}
        caja["especie"] = caja["especie"] or especie_de.get(_norm(caja["variedad"]), "")
        caja.update(nota=nota, fuera=sorted(fuera, key=lambda x: -x[2]), filas=filas,
                    contramuestras=sum(1 for f in filas if _es_contramuestra(f)))
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
        fuera = [f"{c.get('header')}: {_valor(fila.get(c.get('key')))} {EMOJI_CELDA[_color(fila, c.get('key'))]}"
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
            if _color(f, c.get("key")) >= 2:
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
