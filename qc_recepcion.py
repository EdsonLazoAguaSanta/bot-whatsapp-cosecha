"""Calidad de RECEPCIÓN desde QC Manager (Agrez): las inspecciones QC que se hacen a la fruta al llegar a las plantas.

Lo que se vio en los datos reales el 24-11-2026 (y decidió Edson ese día):
- Una recepción (un camión o lote: código, lote, guía, productor con su CSG, variedad, kilos, bins o totes, planta,
  temperaturas) tiene una MUESTRA y a veces una CONTRAMUESTRA (campo `tipo`). Cada muestra trae los defectos (conteo
  y %), % de calidad y de condición, % exportable y comercial, y la NOTA de la recepción (1 la mejor, 4 la peor,
  según Edson) con su causal; la nota va en la MUESTRA (la contramuestra casi siempre viene sin nota).
- Colores, calibres y firmezas son la distribución en % de cada muestra (por `muestra_id`); el Brix viene fruto por
  fruto (30 a 45 frutos por muestra). En carozos, Brix y presión (psi) vienen fruto por fruto en «firmezas».
- Cada llamada trae la temporada completa (sin filtro de fechas): se guarda 20 minutos en memoria. El día es la fecha
  de recepción; el campo `temporada` no se usa (trae recepciones de fines de octubre de 2026 marcadas 2025/2026).
- Plantas: El Carmelo, Lisonjera, El Parque y Greenvic (Almahue todavía no está en la API). Trae también fruta de
  OTRAS exportadoras: el bot la deja solo a quien ve otras exportadoras en esa planta (regla de siempre).
- El CSG es el mismo código de productor de Control_EAS (dbo.Expo-Growers): el alcance por fundos va por nombre o CSG.
- La URL lleva el token: NUNCA se escribe en el log. URL y tokens solo en el .env (el repositorio es público).
"""
import html
import logging
import os
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import requests

logger = logging.getLogger(__name__)

TIMEOUT = 120
TTL = 20 * 60          # segundos: cada endpoint trae la temporada completa
MAX_DIAS = 400

# grupo -> (ruta con {tipo}, variable del token por tipo o una para todo el grupo, tipos)
GRUPOS = {
    "cerezas": ("/cerezas/recepciones/{tipo}/", "QCM_TOKEN_CEREZAS",
                ("muestras", "colores", "calibres", "brix", "firmezas")),
    "arandanos": ("/arandanos/recepciones/{tipo}/", "QCM_TOKEN_ARANDANOS",
                  ("muestras", "colores", "calibres", "brix", "firmezas")),
    "citricos": ("/citricos/recepciones/{tipo}/", "QCM_TOKEN_CITRICOS", ("muestras", "colores", "calibres", "brix")),
    "carozos": ("/recepciones/carozos/{tipo}/", "QCM_TOKEN_CAROZOS_{TIPO}", ("muestras", "firmezas", "calibres")),
}
NOMBRE_GRUPO = {"cerezas": "Cereza", "arandanos": "Arándano", "citricos": "Cítricos", "carozos": "Carozos"}
FIRMEZAS_ORDEN = ("MUY FIRME", "FIRME", "SENSITIVO", "BLANDO", "MUY BLANDO")
# Columnas «_porcent» de la muestra que NO son defectos.
NO_DEFECTOS = {"sum_calidad", "sum_condicion", "exportable", "comercial"}


class ErrorQC(Exception):
    """Un problema con QC Manager que se le puede contar tal cual a quien consulta."""


def _env(nombre):
    return (os.getenv(nombre) or "").strip()


def _url():
    url = _env("QCM_API_URL").rstrip("/")
    return url if url.startswith("https://") else ""


def _token(grupo, tipo):
    return _env(GRUPOS[grupo][1].replace("{TIPO}", tipo.upper()))


def configurada():
    return bool(_url() and _token("cerezas", "muestras"))


def _norm(texto):
    sin_tildes = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode()
    return " ".join(sin_tildes.upper().split())


def _num(valor):
    try:
        return None if valor in (None, "", "None") else float(valor)
    except (TypeError, ValueError):
        return None


def _texto(valor):
    return "" if valor in (None, "None") else str(valor).strip()


def _dia(fila):
    for campo in ("fecha_recepcion", "fecha_inspeccion", "fecha_cosecha"):
        t = _texto(fila.get(campo))
        if len(t) >= 10:
            try:
                return date.fromisoformat(t[:10])
            except ValueError:
                pass
    return None


# ------------------------------------------------------------------------------------------------- descarga

_cache = {}
_candado = threading.Lock()


def _bajar(grupo, tipo):
    token = _token(grupo, tipo)
    if not token:
        return []
    url = f"{_url()}{GRUPOS[grupo][0].format(tipo=tipo)}{token}"
    inicio = time.monotonic()
    try:
        r = requests.get(url, timeout=TIMEOUT)
    except requests.Timeout:
        raise ErrorQC("QC Manager no respondió a tiempo. Intenta de nuevo en un rato.")
    except requests.RequestException as e:
        logger.error(f"QC Manager: error de conexión en {grupo}/{tipo} ({type(e).__name__})")  # sin la URL: lleva el token
        raise ErrorQC("No pude conectarme a QC Manager. Intenta de nuevo en un rato.")
    if r.status_code != 200:
        logger.error(f"QC Manager: {grupo}/{tipo} respondió {r.status_code}")
        raise ErrorQC(f"QC Manager respondió con un error ({r.status_code}).")
    try:
        datos = (r.json() or {}).get("data")
    except ValueError:
        raise ErrorQC("QC Manager respondió algo que no entiendo.")
    if not isinstance(datos, list):
        raise ErrorQC("QC Manager respondió algo que no entiendo.")
    logger.info(f"QC Manager: {grupo}/{tipo} {len(datos)} filas en {time.monotonic() - inicio:.1f} s")
    return datos


def _endpoint(grupo, tipo):
    """Las filas de un endpoint, de la memoria si tienen menos de TTL. Si QC Manager falla y hay una copia anterior,
    se usa esa (y se avisa en el log)."""
    clave = (grupo, tipo)
    with _candado:
        guardado = _cache.get(clave)
    if guardado and time.monotonic() - guardado[0] < TTL:
        return guardado[1]
    try:
        datos = _bajar(grupo, tipo)
    except ErrorQC:
        if guardado:
            logger.warning(f"QC Manager: uso la copia anterior de {grupo}/{tipo}")
            return guardado[1]
        raise
    with _candado:
        _cache[clave] = (time.monotonic(), datos)
    return datos


def _grupo_completo(grupo):
    tipos = GRUPOS[grupo][2]
    with ThreadPoolExecutor(max_workers=len(tipos)) as pool:
        resultados = dict(zip(tipos, pool.map(lambda t: _endpoint(grupo, t), tipos)))
    return resultados


# ------------------------------------------------------------------------------------------------- modelo

def _nombre_defecto(clave):
    if clave == "calibre_l":
        return "Calibre L"
    texto = clave.replace("dano", "daño").replace("_", " ")
    return texto[:1].upper() + texto[1:]


def _limpiar_html(texto):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", _texto(texto))).split())


def muestras(grupos=None):
    """Todas las muestras de recepción, ya armadas: [{datos de la recepción, tipo, nota, causal, %, defectos {nombre:
    %}, brix [frutos], colores {color: %}, calibres {calibre: %}, firmezas {firmeza: %} (promedio de sus grupos de
    color), firmezas_por_color, psi [frutos, carozos]}]."""
    salida = []
    for grupo in (grupos or [g for g in GRUPOS if _token(g, "muestras")]):
        datos = _grupo_completo(grupo)
        detalle = {}
        for tipo, filas in datos.items():
            if tipo == "muestras":
                continue
            for f in filas:
                if f.get("muestra_id") is not None:
                    detalle.setdefault(f["muestra_id"], {}).setdefault(tipo, []).append(f)
        for f in datos.get("muestras") or []:
            d = detalle.get(f.get("id"), {})
            defectos = {}
            for k in f:
                if k.endswith("_porcent") and k[:-8] not in NO_DEFECTOS:
                    v = _num(f.get(k))
                    if v:
                        nombre = (_texto(f.get(k[:-8] + "_nombre")) or _nombre_defecto(k[:-8])) if k.startswith("extra_") \
                            else _nombre_defecto(k[:-8])
                        defectos[nombre] = v
            colores = {}
            for c in d.get("colores", []):
                colores[_texto(c.get("color"))] = colores.get(_texto(c.get("color")), 0) + (_num(c.get("porcentaje")) or 0)
            calibres = {}
            for c in sorted(d.get("calibres", []), key=lambda c: _num(c.get("numero_orden")) or 0):
                calibres[_texto(c.get("calibre"))] = calibres.get(_texto(c.get("calibre")), 0) + (_num(c.get("porcentaje")) or 0)
            brix = [_num(b.get("brix")) for b in d.get("brix", []) if (_num(b.get("brix")) or 0) > 0.1]
            por_color, psi = {}, []
            for c in d.get("firmezas", []):
                if grupo == "carozos":  # fruto por fruto: Brix y presión
                    if (_num(c.get("brix")) or 0) > 0.1:
                        brix.append(_num(c.get("brix")))
                    medidas = [_num(c.get(k)) for k in ("mejilla_1", "mejilla_2") if _num(c.get(k))]
                    if medidas:
                        psi.append(sum(medidas) / len(medidas))
                    continue
                g = por_color.setdefault(_texto(c.get("color")) or "(sin color)", {})
                g[_texto(c.get("firmeza"))] = g.get(_texto(c.get("firmeza")), 0) + (_num(c.get("porcentaje")) or 0)
            firmezas = {}
            for g in por_color.values():
                for k, v in g.items():
                    firmezas[k] = firmezas.get(k, 0) + v / len(por_color)
            nota = _num(f.get("nota_muestra"))
            salida.append({
                "grupo": grupo, "id": f.get("id"), "recepcion_id": f.get("recepcion_id"), "codigo": _texto(f.get("codigo")),
                "lote": _texto(f.get("lote")), "guia": _texto(f.get("guia")), "fecha": _dia(f), "hora": _texto(f.get("hora")),
                "planta": _texto(f.get("planta")), "packing": _texto(f.get("planta")),
                "exportadora": _texto(f.get("exportadora")), "agua_santa": "AGUA SANTA" in _norm(f.get("exportadora")),
                "productor": _texto(f.get("productor")), "codigo_productor": _texto(f.get("csg")),
                "campo": _texto(f.get("campo")), "especie": _texto(f.get("especie")), "variedad": _texto(f.get("variedad")),
                "cuartel": _texto(f.get("cuartel")), "kilos": _num(f.get("kilos")) or 0, "bins": _num(f.get("bins")),
                "totes": _num(f.get("totes")), "envase": _texto(f.get("envase_fruta")),
                "destino": _texto(f.get("destino_planta")), "temp_arribo": _num(f.get("temp_arribo")),
                "tipo": _texto(f.get("tipo")).upper() or "MUESTRA", "numero": f.get("numero"),
                "nota": int(nota) if nota else None, "causal": _texto(f.get("causal_nota_muestra")),
                "exportable": _num(f.get("exportable_porcent")), "comercial": _num(f.get("comercial_porcent")),
                "calidad": _num(f.get("sum_calidad_porcent")), "condicion": _num(f.get("sum_condicion_porcent")),
                "peso_muestra": _num(f.get("peso_muestra")), "frutos_muestra": _num(f.get("frutos_muestra")),
                "defectos": defectos, "brix": brix, "colores": colores, "calibres": calibres, "firmezas": firmezas,
                "firmezas_por_color": por_color, "psi": psi, "observaciones": _limpiar_html(f.get("observaciones")),
            })
    return salida


def recepciones(desde, hasta, permitido, grupos=None):
    """{recepcion: [muestras]} del período, solo lo que esa persona puede ver (permitido(muestra) -> bool)."""
    if hasta < desde:
        desde, hasta = hasta, desde
    if (hasta - desde).days + 1 > MAX_DIAS:
        raise ErrorQC(f"Consulto hasta {MAX_DIAS} días seguidos. Pide un rango más corto.")
    salida = {}
    for m in muestras(grupos):
        if m["fecha"] and desde <= m["fecha"] <= hasta and permitido(m):
            salida.setdefault((m["grupo"], m["recepcion_id"]), []).append(m)
    for lista in salida.values():
        lista.sort(key=lambda m: (m["tipo"] != "MUESTRA", m["numero"] or 0))
    return salida


def nota_de(lista):
    """La nota de una recepción: la de su MUESTRA (la contramuestra casi nunca trae)."""
    return next((m["nota"] for m in lista if m["nota"]), None)


def causales_de(lista):
    """Las causales de la nota, separadas y sin tildes («FIRMEZA-SOLIDOS SOLUBLES» son dos)."""
    texto = next((m["causal"] for m in lista if m["causal"]), "")
    return [c for c in (_norm(p) for p in re.split(r"[-/,;]+", texto)) if c]


def promedio(valores):
    v = [x for x in valores if isinstance(x, (int, float))]
    return sum(v) / len(v) if v else None


def _coincide(buscado, valor):
    b, v = _norm(buscado), _norm(valor)
    return bool(b) and b in v


def _dec(x, d=1):
    return "—" if x is None else f"{x:.{d}f}".replace(".", ",")


def _miles(x):
    return f"{x:,.0f}".replace(",", ".")


def filtrar(recs, planta=None, productor=None, variedad=None, especie=None, lote=None):
    filtros = {"planta": planta, "productor": productor, "variedad": variedad, "especie": especie}
    salida = {}
    for clave, lista in recs.items():
        m = lista[0]
        if any(_texto(b) and not (_coincide(b, m[k]) or (k == "especie" and _coincide(b, NOMBRE_GRUPO[m["grupo"]])))
               for k, b in filtros.items()):
            continue
        if _texto(lote) and not any(_norm(lote) in (_norm(m["lote"]), _norm(m["guia"]), _norm(m["codigo"])) for m in lista):
            continue
        salida[clave] = lista
    return salida


def estadisticas(listas):
    """Cifras de un grupo de recepciones: cantidad, kilos, notas, % exportable, defectos, Brix, firmeza, causales."""
    todas = [m for lista in listas for m in lista]
    notas = {k: sum(1 for l in listas if nota_de(l) == k) for k in (1, 2, 3, 4)}
    con_nota = sum(notas.values())
    brix = [b for m in todas for b in m["brix"]]
    defectos = {}
    for m in todas:
        for k, v in m["defectos"].items():
            defectos.setdefault(k, []).append(v)
    causales = {}
    for l in listas:
        if nota_de(l) == 4:
            for c in causales_de(l):
                causales[c] = causales.get(c, 0) + 1
    firmezas = {}
    con_firmeza = [m for m in todas if m["firmezas"]]
    for m in con_firmeza:
        for k, v in m["firmezas"].items():
            firmezas.setdefault(k, []).append(v)
    return {
        "recepciones": len(listas), "muestras": len(todas), "kilos": sum(l[0]["kilos"] for l in listas),
        "notas": notas, "con_nota": con_nota,
        "pct_nota": {k: 100 * n / con_nota for k, n in notas.items()} if con_nota else {},
        "exportable": promedio([m["exportable"] for m in todas]),
        "calidad": promedio([m["calidad"] for m in todas]), "condicion": promedio([m["condicion"] for m in todas]),
        "brix": promedio(brix), "frutos_brix": len(brix),
        # Promedio de cada defecto sobre TODAS las muestras (si una no lo tuvo, cuenta 0).
        "defectos": sorted(((k, sum(v) / len(todas)) for k, v in defectos.items()), key=lambda x: -x[1]),
        "causales_nota4": sorted(causales.items(), key=lambda x: (-x[1], x[0])),
        # Promedio sobre las muestras que midieron firmeza (si una no tuvo esa categoría, cuenta 0).
        "firmezas": {k: sum(v) / len(con_firmeza) for k, v in firmezas.items()} if con_firmeza else {},
        "psi": promedio([p for m in todas for p in m["psi"]]),
    }


def _linea_recepcion(lista):
    m = lista[0]
    datos = [f"{m['fecha']:%d-%m}" if m["fecha"] else "", m["hora"], m["planta"], m["productor"], m["variedad"],
             f"lote {m['lote']}" if m["lote"] else "", f"guía {m['guia']}" if m["guia"] else "",
             f"{_miles(m['kilos'])} kg" if m["kilos"] else ""]
    nota = nota_de(lista)
    exp = promedio([x["exportable"] for x in lista])
    return (f"Nota {nota or '—'} · " + " · ".join(d for d in datos if d)
            + (f" · exportable {_dec(exp)} %" if exp is not None else "")
            + (f" · causal: {', '.join(c.lower() for c in causales_de(lista))}" if causales_de(lista) else ""))


def resumen(desde, hasta, permitido, agrupar=None, solo_nota4=True, max_lineas=15, **filtros):
    """Texto para Claude: la calidad de recepción del período con filtros, y por grupo si se pide."""
    recs = filtrar(recepciones(desde, hasta, permitido), **filtros)
    periodo = desde.strftime("%d-%m-%Y") + ("" if hasta == desde else f" al {hasta:%d-%m-%Y}")
    if not recs:
        return f"QC Manager no tiene recepciones inspeccionadas para {periodo} con esos filtros (que esta persona pueda ver)."
    listas = list(recs.values())
    e = estadisticas(listas)
    lineas = [f"Calidad de recepción (QC Manager) · {periodo}",
              f"{e['recepciones']} recepciones inspeccionadas ({_miles(e['kilos'])} kg recibidos), {e['muestras']} muestras",
              "Nota de la recepción (1 la mejor, 4 la peor): " + " · ".join(
                  f"nota {k}: {e['notas'][k]} ({_dec(e['pct_nota'].get(k, 0))} %)" for k in (1, 2, 3, 4))
              + (f" · sin nota: {e['recepciones'] - e['con_nota']}" if e["recepciones"] > e["con_nota"] else ""),
              f"% exportable promedio: {_dec(e['exportable'])} % · calidad {_dec(e['calidad'])} % · condición "
              f"{_dec(e['condicion'])} % (defectos sumados)"]
    if e["brix"] is not None:
        lineas.append(f"Brix en recepción: {_dec(e['brix'])}° ({e['frutos_brix']} frutos medidos)")
    if e["firmezas"]:
        lineas.append("Firmeza (promedio de las muestras): " + " · ".join(
            f"{k.lower()} {_dec(e['firmezas'][k])} %" for k in FIRMEZAS_ORDEN if k in e["firmezas"]))
    if e["psi"] is not None:
        lineas.append(f"Presión promedio (carozos, mejillas): {_dec(e['psi'])} lb")
    if e["defectos"]:
        lineas.append("Defectos principales (promedio %): " + "; ".join(f"{k} {_dec(v)} %" for k, v in e["defectos"][:8]))
    if e["causales_nota4"]:
        lineas.append("Causales de las nota 4: " + "; ".join(f"{c.lower()} ({n})" for c, n in e["causales_nota4"][:6]))
    otras = sum(1 for l in listas if not l[0]["agua_santa"])
    if otras:
        lineas.append(f"Incluye {otras} recepciones de otras exportadoras (dilo siempre y no las sumes a Agua Santa sin avisar).")
    campo = {"productor": "productor", "variedad": "variedad", "planta": "planta", "dia": "fecha", "especie": "especie",
             "exportadora": "exportadora"}.get(_norm(agrupar).lower()) if _texto(agrupar) else None
    if campo:
        grupos = {}
        for l in listas:
            grupos.setdefault(l[0][campo], []).append(l)
        lineas.append(f"Por {agrupar}:")
        for nombre, g in sorted(grupos.items(), key=lambda x: (str(x[0]) if campo == "fecha" else "", -len(x[1]))):
            s = estadisticas(g)
            lineas.append(f"- {nombre:%d-%m-%Y}" if campo == "fecha" else f"- {nombre}")
            lineas[-1] += (f": {s['recepciones']} recepciones, {_miles(s['kilos'])} kg, notas 1/2/3/4: "
                           + "/".join(str(s["notas"][k]) for k in (1, 2, 3, 4))
                           + (f" (sin nota {s['recepciones'] - s['con_nota']})" if s["recepciones"] > s["con_nota"] else "")
                           + f", nota 4 {_dec(s['pct_nota'].get(4, 0))} %, exportable {_dec(s['exportable'])} %, "
                           f"Brix {_dec(s['brix'])}°")
    elegidas = [l for l in listas if (nota_de(l) == 4 or not solo_nota4)]
    elegidas.sort(key=lambda l: (l[0]["fecha"] or date.min, l[0]["hora"]), reverse=True)
    if elegidas:
        lineas.append("Recepciones con nota 4:" if solo_nota4 else "Recepciones:")
        lineas += [f"- {_linea_recepcion(l)}" for l in elegidas[:max_lineas]]
        if len(elegidas) > max_lineas:
            lineas.append(f"(+{len(elegidas) - max_lineas} más; acota por planta, productor o día)")
    lineas.append("Nota 1 la mejor y 4 la peor (la pone QC en la muestra). Los % son promedios de muestra y contramuestra. "
                  "Para una recepción completa (defectos, calibres, color, firmeza, Brix): detalle_qc_recepcion con su "
                  "lote, guía o código.")
    return "\n".join(lineas)


def detalle(desde, hasta, permitido, lote):
    """Texto para Claude: una recepción completa por su lote, guía o código (si calza más de una, las lista)."""
    recs = filtrar(recepciones(desde, hasta, permitido), lote=lote)
    if not recs:
        return f"No encontré la recepción «{lote}» entre el {desde:%d-%m-%Y} y el {hasta:%d-%m-%Y} (o es de algo que esta persona no ve)."
    if len(recs) > 1:
        return (f"«{lote}» calza con {len(recs)} recepciones: pregunta cuál (por fecha, planta o productor) y vuelve a "
                "llamar con su código:\n" + "\n".join(f"- {l[0]['codigo']}: {_linea_recepcion(l)}" for l in recs.values()))
    lista = next(iter(recs.values()))
    m = lista[0]
    lineas = [f"Recepción {m['codigo']} · {_linea_recepcion(lista)}",
              "Datos: " + "; ".join(x for x in (
                  f"exportadora {m['exportadora']}", f"CSG {m['codigo_productor']}" if m["codigo_productor"] else "",
                  f"cuartel {m['cuartel']}" if m["cuartel"] else "",
                  f"{_miles(m['bins'])} bins" if m["bins"] else "", f"{_miles(m['totes'])} totes" if m["totes"] else "",
                  f"envase {m['envase']}" if m["envase"] else "", f"destino {m['destino'].lower()}" if m["destino"] else "",
                  f"T° arribo {_dec(m['temp_arribo'])} °C" if m["temp_arribo"] is not None else "") if x)]
    for x in lista:
        partes = [f"{x['tipo'].capitalize()}: nota {x['nota'] or '—'}" + (f" ({x['causal'].lower()})" if x["causal"] else ""),
                  f"exportable {_dec(x['exportable'])} %, comercial {_dec(x['comercial'])} %, calidad {_dec(x['calidad'])} %, "
                  f"condición {_dec(x['condicion'])} %"]
        if x["defectos"]:
            partes.append("defectos: " + "; ".join(f"{k} {_dec(v)} %" for k, v in sorted(x["defectos"].items(), key=lambda d: -d[1])))
        if x["brix"]:
            partes.append(f"Brix {_dec(promedio(x['brix']))}° ({len(x['brix'])} frutos, {_dec(min(x['brix']))} a {_dec(max(x['brix']))})")
        if x["firmezas_por_color"]:
            partes.append("firmeza: " + " | ".join(
                f"{color.lower()}: " + ", ".join(f"{k.lower()} {_dec(v)} %" for k, v in g.items() if v)
                for color, g in x["firmezas_por_color"].items()))
        if x["psi"]:
            partes.append(f"presión {_dec(promedio(x['psi']))} lb ({len(x['psi'])} frutos)")
        if x["calibres"]:
            partes.append("calibres: " + ", ".join(f"{k} {_dec(v)} %" for k, v in x["calibres"].items() if v))
        if x["colores"]:
            partes.append("color: " + ", ".join(f"{k.lower()} {_dec(v)} %" for k, v in x["colores"].items() if v))
        if x["observaciones"]:
            partes.append(f"observaciones: {x['observaciones']}")
        lineas.append(" · ".join(partes))
    return "\n".join(lineas)
