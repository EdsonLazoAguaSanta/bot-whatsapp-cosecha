"""
Bases externas que el Asistente puede consultar además de la de cosecha (Control_EAS).

Cada base se declara en BASES (al final de este archivo) con:
  - su conexión, leída del .env con un prefijo propio. Para Cierre:
        CIERRE_SQL_SERVER, CIERRE_SQL_PORT (opcional), CIERRE_SQL_DATABASE (por defecto Presupuesto),
        CIERRE_SQL_USER (por defecto claude_movil), CIERRE_SQL_PASSWORD, CIERRE_SQL_DRIVER (opcional)
    Mientras falte el servidor o la clave, la base queda "sin configurar" y no se le ofrece a nadie.
  - un interruptor de mantención propio: el archivo BASE_DESCONECTADA_<ID> junto al bot, que se crea
    y se borra con /admin/base?base=<id>&estado=desconectar|conectar. No toca la base de cosecha.
  - dos herramientas para Claude: consultar_<id> (UNA consulta SELECT) y ver_modelo_<id> (las vistas
    y columnas que puede leer el login de esa base).

Quién puede usar cada base lo decide el bot (perfil + tabla numeros_bases). Este módulo no sabe de
números de WhatsApp: recibe el correo de la persona cuando la base filtra por usuario (Cierre).

Seguridad, de afuera hacia adentro:
  1. El bot solo ofrece y ejecuta las herramientas de las bases habilitadas para ese número.
  2. Cada base usa un login SQL de SOLO LECTURA y acotado. Esa es la barrera real: la validación de
     este módulo no reemplaza los permisos del login. (Cierre: claude_movil, SELECT solo sobre el
     esquema movil; el mismo login del conector Cierre SQL.)
  3. Cierre: el correo del usuario se fija en la sesión SQL (de solo lectura: no se puede cambiar
     dentro de la sesión) y las vistas de movil filtran por los grupos que ese correo tiene en
     dbo.Cierre_UsuarioAcceso.
  4. Aquí solo se acepta UNA consulta SELECT (o WITH ... SELECT) por llamada, sin nombres de 3 partes.
"""
from __future__ import annotations

import ast
import datetime as dt
import decimal
import logging
import os
import re
import time
from pathlib import Path

import pyodbc

logger = logging.getLogger("bases_externas")

CARPETA = Path(__file__).resolve().parent


def _env(nombre, defecto=None):
    valor = os.getenv(nombre, defecto)
    if isinstance(valor, str):
        valor = valor.strip()
    return valor or None


class ErrorBase(Exception):
    """Error con un mensaje que se le puede mostrar tal cual al usuario (y a Claude)."""


# ---------------------------------------------------------------------------
# Validación de la consulta: la misma regla del conector Cierre SQL (C:\ClaudeCierre\server.py,
# preparar_consulta), para que una consulta que funciona en Claude funcione igual aquí.
# ---------------------------------------------------------------------------
_PALABRAS_PROHIBIDAS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE|DENY|"
    r"BACKUP|RESTORE|DBCC|SHUTDOWN|KILL|RECONFIGURE|WAITFOR|OPENQUERY|OPENROWSET|OPENDATASOURCE|"
    r"OPENXML|BULK|USE|SETUSER|REVERT|INTO|DECLARE|SET|CURSOR|FETCH|GOTO|TRAN|TRANSACTION|COMMIT|"
    r"ROLLBACK|SAVE|PRINT|RAISERROR|THROW)\b",
    re.IGNORECASE,
)
_PROC_SISTEMA = re.compile(r"\b(sp|xp)_\w+", re.IGNORECASE)
_IDENT = r"(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z_#@][\w@$#]*)"
_TRES_PARTES = re.compile(rf"{_IDENT}\s*\.\s*(?:{_IDENT})?\s*\.\s*{_IDENT}")
_DBO = re.compile(r"(\[dbo\]|\bdbo)\s*\.", re.IGNORECASE)
MAX_LARGO_CONSULTA = 12000


def _segmentos(sql):
    """Divide el texto en (es_codigo, texto) separando literales y comentarios. Los comentarios
    salen como un espacio: no llegan a SQL Server."""
    i, n, inicio = 0, len(sql), 0
    while i < n:
        c = sql[i]
        if c == "'" or (c in "Nn" and i + 1 < n and sql[i + 1] == "'"):
            if i > inicio:
                yield True, sql[inicio:i]
            j = i + (2 if c in "Nn" else 1)
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            yield False, sql[i:j + 1]
            i = inicio = j + 1
        elif c == "-" and sql[i:i + 2] == "--":
            if i > inicio:
                yield True, sql[inicio:i]
            j = sql.find("\n", i)
            j = n if j == -1 else j
            yield False, " "
            i = inicio = j
        elif c == "/" and sql[i:i + 2] == "/*":
            if i > inicio:
                yield True, sql[inicio:i]
            j = sql.find("*/", i + 2)
            j = n if j == -1 else j + 2
            yield False, " "
            i = inicio = j
        else:
            i += 1
    if inicio < n:
        yield True, sql[inicio:]


def preparar_consulta(sql, esquema="dbo"):
    """Valida que sea un único SELECT (o WITH ... SELECT) de solo lectura y, si la base usa otro
    esquema, cambia 'dbo.' por ese esquema (Claude a veces escribe dbo.Cierre_Gastos). Devuelve la
    consulta lista para ejecutar o lanza ErrorBase con el motivo."""
    if not sql or not str(sql).strip():
        raise ErrorBase("La consulta está vacía.")
    sql = str(sql)
    if len(sql) > MAX_LARGO_CONSULTA:
        raise ErrorBase("La consulta es demasiado larga: simplifícala o divídela en varias.")
    cambiar_dbo = bool(esquema) and esquema.lower() != "dbo"
    partes, codigo = [], []
    for es_codigo, texto in _segmentos(sql):
        if es_codigo:
            if cambiar_dbo:
                texto = _DBO.sub(f"{esquema}.", texto)
            codigo.append(texto)
        partes.append(texto)
    limpio = " ".join(codigo).strip().rstrip(";").strip()

    if ";" in limpio:
        raise ErrorBase("Solo se permite una consulta por llamada (sin ';' intermedios).")
    if not re.match(r"^(SELECT|WITH)\b", limpio, re.IGNORECASE):
        raise ErrorBase("Solo se permiten consultas que empiecen con SELECT o WITH.")
    m = _PALABRAS_PROHIBIDAS.search(limpio) or _PROC_SISTEMA.search(limpio)
    if m:
        raise ErrorBase(f"Palabra no permitida en la consulta: {m.group(0).upper()}. Solo lectura con SELECT.")
    if _TRES_PARTES.search(limpio):
        raise ErrorBase("Usa nombres de 1 o 2 partes (ej. Cierre_Gastos o g.Monto): "
                        "no se puede leer otras bases ni servidores.")
    return "".join(partes).strip().rstrip(";").strip()


# ---------------------------------------------------------------------------
# Formato de resultados y errores
# ---------------------------------------------------------------------------
def _fmt(v):
    """Valor tal cual para Claude: punto decimal, sin separador de miles."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, decimal.Decimal):
        return format(v.normalize(), "f")
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return f"{v:.6f}".rstrip("0").rstrip(".")
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ", timespec="seconds")
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return "0x" + bytes(v[:16]).hex() + ("…" if len(v) > 16 else "")
    return str(v).replace("|", "/").replace("\r", " ").replace("\n", " ")


def _detalle_error(e):
    """Texto de un error de pyodbc sin los prefijos [Microsoft][ODBC ...]."""
    texto = str(e.args[1] if len(getattr(e, "args", ())) > 1 else e)
    texto = re.sub(r"\[[^\]]*\]", "", texto)
    texto = re.sub(r"\(\d+\)\s*\(SQL\w+\)", "", texto)
    return " ".join(texto.split())[:500]


def _llave(valor):
    """Valor de una cadena de conexión ODBC entre llaves (por si la clave trae ';' o '}')."""
    return "{" + str(valor).replace("}", "}}") + "}"


def _texto_de_archivo(ruta, nombre):
    """Lee una constante de texto de un .py SIN ejecutarlo (ej. REGLAS_MODELO del conector Cierre
    SQL), para que el Asistente use siempre las mismas reglas que el conector. None si no se puede."""
    if not ruta:
        return None
    try:
        arbol = ast.parse(Path(ruta).read_text(encoding="utf-8"), filename=str(ruta))
    except (OSError, SyntaxError, ValueError) as e:
        logger.warning(f"No pude leer {nombre} de {ruta}: {e}")
        return None
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == nombre for t in nodo.targets)
                and isinstance(nodo.value, ast.Constant) and isinstance(nodo.value.value, str)):
            return nodo.value.value.strip()
    logger.warning(f"{ruta} no tiene el texto {nombre}")
    return None


# ---------------------------------------------------------------------------
# Una base externa
# ---------------------------------------------------------------------------
class BaseExterna:
    """Una base SQL Server de solo lectura. Para sumar una base nueva basta una instancia en BASES
    (o una subclase si necesita preparar la sesión por usuario, como Cierre) y sus variables en el
    .env. Quién la ve se decide en el bot: por perfil (PERFILES) o por número (?<id>=si en /admin)."""

    # True si la base filtra los datos según el correo de la persona (Cierre): sin correo no se consulta.
    requiere_email = False
    # Nota al pie de los informes PDF y Excel que leen esta base (ver consulta_tabla).
    nota_informe = ""

    def __init__(self, id, nombre, prefijo_env, descripcion, reglas="", esquema="dbo",
                 base_defecto=None, usuario_defecto=None, max_filas=400):
        self.id = id
        self.nombre = nombre
        self.prefijo = prefijo_env
        self.descripcion = descripcion
        self.reglas = reglas or ""
        self.esquema = esquema
        self.servidor = _env(f"{prefijo_env}_SQL_SERVER")
        self.puerto = _env(f"{prefijo_env}_SQL_PORT")
        self.base = _env(f"{prefijo_env}_SQL_DATABASE", base_defecto)
        self.usuario = _env(f"{prefijo_env}_SQL_USER", usuario_defecto)
        self.clave = _env(f"{prefijo_env}_SQL_PASSWORD")
        self.driver = _env(f"{prefijo_env}_SQL_DRIVER", "ODBC Driver 17 for SQL Server")
        self.max_filas = int(_env(f"{prefijo_env}_MAX_FILAS", str(max_filas)))
        self.max_caracteres = int(_env(f"{prefijo_env}_MAX_CARACTERES", "60000"))
        self.tiempo_max = int(_env(f"{prefijo_env}_TIEMPO_MAX_SQL", "120"))
        self.archivo_mantencion = CARPETA / f"BASE_DESCONECTADA_{id.upper()}"
        self.herramienta_consulta = f"consultar_{id}"
        self.herramienta_modelo = f"ver_modelo_{id}"
        self._definiciones = None

    # --- estado -------------------------------------------------------------
    def faltantes(self):
        """Variables del .env que faltan para poder conectarse."""
        return [f"{self.prefijo}_SQL_{clave}" for clave, valor in (
            ("SERVER", self.servidor), ("DATABASE", self.base), ("USER", self.usuario),
            ("PASSWORD", self.clave)) if not valor]

    @property
    def configurada(self):
        return not self.faltantes()

    def desconectada(self):
        return self.archivo_mantencion.exists()

    def fijar_mantencion(self, desconectar):
        """Modo mantención de ESTA base, sin reiniciar el bot ni tocar la de cosecha."""
        if desconectar:
            self.archivo_mantencion.write_text(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
            logger.warning(f"Base externa '{self.id}' desconectada (modo mantención)")
        elif self.archivo_mantencion.exists():
            self.archivo_mantencion.unlink()
            logger.warning(f"Base externa '{self.id}' reconectada")

    def estado(self):
        if not self.configurada:
            return f"sin configurar (falta {', '.join(self.faltantes())} en el .env)"
        if self.desconectada():
            return "desconectada (modo mantención)"
        return "conectada"

    # --- conexión -----------------------------------------------------------
    def _cadena_conexion(self):
        servidor = f"{self.servidor},{self.puerto}" if self.puerto else self.servidor
        return ";".join([
            f"Driver={_llave(self.driver)}", f"Server={servidor}", f"Database={_llave(self.base)}",
            f"UID={_llave(self.usuario)}", f"PWD={_llave(self.clave)}", f"APP=Asistente-WhatsApp-{self.id}",
        ])

    def _preparar_sesion(self, cur, email):
        """Ajustes de la sesión SQL antes de consultar. Devuelve el alcance del usuario como texto
        (None si la base no filtra por usuario). Las subclases lo cambian (ver BaseCierre)."""
        cur.execute("SET NOCOUNT ON; SET LOCK_TIMEOUT 30000;")
        return None

    def conectar(self, email=None):
        """(conexión, cursor, alcance). Los problemas salen como ErrorBase con un mensaje para el usuario."""
        if not self.configurada:
            raise ErrorBase(f"La base {self.nombre} todavía no está configurada en el Asistente: avisa al administrador.")
        if self.desconectada():
            raise ErrorBase(f"La base {self.nombre} está en mantención por un momento: vuelve a preguntarme en un rato.")
        if self.requiere_email and not email:
            raise ErrorBase(f"Tu usuario del Asistente no tiene un correo registrado, y la base {self.nombre} muestra "
                            "los datos según el correo. Pide al administrador que lo agregue.")
        try:
            conn = pyodbc.connect(self._cadena_conexion(), timeout=15, autocommit=True)
        except pyodbc.Error as e:
            logger.error(f"{self.id}: no se pudo conectar a SQL Server: {_detalle_error(e)}")
            raise ErrorBase(f"No pude conectarme a la base {self.nombre}. Intenta de nuevo en un rato; "
                            "si sigue, avisa al administrador.") from e
        try:
            conn.timeout = self.tiempo_max
            cur = conn.cursor()
            alcance = self._preparar_sesion(cur, email)
        except ErrorBase:
            conn.close()
            raise
        except pyodbc.Error as e:
            conn.close()
            logger.error(f"{self.id}: error preparando la sesión de {email}: {_detalle_error(e)}")
            raise ErrorBase(self._mensaje_error_sql(e)) from e
        return conn, cur, alcance

    def _mensaje_error_sql(self, e):
        estado = e.args[0] if getattr(e, "args", None) else ""
        texto = str(e.args[1] if len(getattr(e, "args", ())) > 1 else e)
        if estado == "HYT00" or "timeout expired" in texto.lower():
            return "La consulta tardó demasiado: filtra más (temporada, fundo, mes) o agrupa."
        m = re.search(r"\((\d+)\)\s*\(SQL", texto)
        codigo = m.group(1) if m else ""
        if codigo == "1222":
            return "La base se está actualizando en este momento. Intenta de nuevo en un minuto."
        if codigo in ("229", "230", "262", "300"):
            return (f"Sin permiso sobre ese objeto: solo se pueden leer las vistas que muestra "
                    f"{self.herramienta_modelo}.")
        return f"SQL Server rechazó la consulta{f' (error {codigo})' if codigo else ''}: {_detalle_error(e)}"

    # --- herramientas para Claude ------------------------------------------
    def herramientas(self):
        """Definiciones para Claude. Se arman una sola vez: siempre iguales, para la caché de prompts."""
        if self._definiciones is None:
            tecnicas = (
                f"Cómo consultar: llama primero a {self.herramienta_modelo} si no tienes a la vista las "
                f"columnas que vas a usar. Envía UNA consulta SELECT (o WITH ... SELECT), con los nombres sin "
                f"esquema y sin ';'. Agrega en SQL (GROUP BY, SUM, TOP) en vez de traer muchas filas: devuelve "
                f"máximo {self.max_filas}. Si da error, lee el mensaje, corrige y reintenta."
            )
            descripcion = "\n\n".join(t for t in (self.descripcion, tecnicas, self.reglas) if t)
            self._definiciones = [
                {
                    "name": self.herramienta_modelo,
                    "description": (f"Muestra las vistas de la base {self.nombre} que puede leer este usuario, con "
                                    f"sus columnas y tipos, y el alcance de sus datos. Úsala antes de escribir una "
                                    f"consulta con {self.herramienta_consulta}."),
                    "input_schema": {"type": "object", "properties": {}},
                },
                {
                    "name": self.herramienta_consulta,
                    "description": descripcion,
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "consulta": {"type": "string",
                                         "description": f"Una consulta T-SQL SELECT (o WITH ... SELECT) sobre las vistas de {self.nombre}."},
                        },
                        "required": ["consulta"],
                    },
                },
            ]
        return self._definiciones

    def ejecutar(self, herramienta, entrada, email=None, quien=""):
        """Ejecuta una de sus herramientas y devuelve el texto para Claude (los errores, como 'ERROR: ...')."""
        inicio = time.perf_counter()
        if herramienta == self.herramienta_modelo:
            return self._ver_modelo(email, quien, inicio)
        if herramienta == self.herramienta_consulta:
            return self._consultar((entrada or {}).get("consulta"), email, quien, inicio)
        return f"ERROR: la base {self.nombre} no tiene la herramienta {herramienta}."

    def _registrar(self, quien, email, herramienta, inicio, filas=None, error=None, sql=None):
        ms = int((time.perf_counter() - inicio) * 1000)
        logger.info("%s\t%s\t%s\t%s\t%s ms\tfilas=%s\t%s\t%s", self.id, quien or "-", email or "-", herramienta,
                    ms, filas, error or "ok", (sql or "").replace("\n", " ")[:1500])
        return ms

    def _consultar(self, consulta, email, quien, inicio):
        herramienta = self.herramienta_consulta
        try:
            sql = preparar_consulta(consulta, self.esquema)
        except ErrorBase as e:
            self._registrar(quien, email, herramienta, inicio, error=f"rechazada: {e}", sql=consulta)
            return f"ERROR: {e}"
        try:
            conn, cur, alcance = self.conectar(email)
        except ErrorBase as e:
            self._registrar(quien, email, herramienta, inicio, error=str(e), sql=sql)
            return f"ERROR: {e}"
        try:
            cur.execute(sql)
            if cur.description is None:
                self._registrar(quien, email, herramienta, inicio, filas=0, sql=sql)
                return "La consulta no devolvió una tabla."
            columnas = [d[0] or f"columna_{i + 1}" for i, d in enumerate(cur.description)]
            filas = cur.fetchmany(self.max_filas + 1)
        except pyodbc.Error as e:
            msg = self._mensaje_error_sql(e)
            self._registrar(quien, email, herramienta, inicio, error=msg, sql=sql)
            return f"ERROR: {msg}"
        finally:
            conn.close()
        truncado = len(filas) > self.max_filas
        filas = filas[:self.max_filas]
        self._registrar(quien, email, herramienta, inicio, filas=len(filas), sql=sql)
        if not filas:
            return "La consulta no devolvió filas (sin datos para esos filtros, o fuera del alcance del usuario)."
        cabecera = f"{len(filas)} fila(s)" + (f" · alcance: {alcance}" if alcance else "") + ":"
        cuerpo = "\n".join([" | ".join(columnas)] + [" | ".join(_fmt(v) for v in f) for f in filas])
        if len(cuerpo) > self.max_caracteres:
            cuerpo = cuerpo[:self.max_caracteres] + "\n… (resultado cortado por tamaño: agrupa o filtra más)"
        nota = (f"\n(Se muestran solo las primeras {self.max_filas} filas: agrupa más o usa TOP si necesitas "
                f"el total.)") if truncado else ""
        return f"{cabecera}\n{cuerpo}{nota}"

    def consulta_tabla(self, consulta, email=None, quien="", max_filas=None):
        """Para los informes PDF y Excel del bot (generar_informe con fuente=<id>): la misma validación,
        conexión y filtro por correo que consultar_<id>, pero devuelve la tabla en vez de texto:
        ({"columnas", "filas", "truncado", "alcance"}, None) o (None, motivo)."""
        inicio = time.perf_counter()
        herramienta = f"informe_{self.id}"
        tope = max(1, int(max_filas or self.max_filas))
        try:
            sql = preparar_consulta(consulta, self.esquema)
        except ErrorBase as e:
            self._registrar(quien, email, herramienta, inicio, error=f"rechazada: {e}", sql=consulta)
            return None, str(e)
        try:
            conn, cur, alcance = self.conectar(email)
        except ErrorBase as e:
            self._registrar(quien, email, herramienta, inicio, error=str(e), sql=sql)
            return None, str(e)
        try:
            cur.execute(sql)
            if cur.description is None:
                self._registrar(quien, email, herramienta, inicio, filas=0, sql=sql)
                return None, "La consulta no devolvió una tabla."
            columnas = [d[0] or f"columna_{i + 1}" for i, d in enumerate(cur.description)]
            filas = cur.fetchmany(tope + 1)
        except pyodbc.Error as e:
            msg = self._mensaje_error_sql(e)
            self._registrar(quien, email, herramienta, inicio, error=msg, sql=sql)
            return None, msg
        finally:
            conn.close()
        self._registrar(quien, email, herramienta, inicio, filas=min(len(filas), tope), sql=sql)
        return {"columnas": columnas, "filas": [list(f) for f in filas[:tope]], "truncado": len(filas) > tope,
                "alcance": alcance}, None

    def _ver_modelo(self, email, quien, inicio):
        herramienta = self.herramienta_modelo
        try:
            conn, cur, alcance = self.conectar(email)
        except ErrorBase as e:
            self._registrar(quien, email, herramienta, inicio, error=str(e))
            return f"ERROR: {e}"
        try:
            cur.execute(
                "SELECT o.name, c.name, TYPE_NAME(c.user_type_id) FROM sys.objects o "
                "JOIN sys.columns c ON c.object_id = o.object_id "
                "WHERE o.schema_id = SCHEMA_ID(?) AND o.type IN ('V', 'U') ORDER BY o.name, c.column_id;",
                self.esquema,
            )
            objetos = {}
            for objeto, columna, tipo in cur.fetchall():
                objetos.setdefault(objeto, []).append(f"{columna} ({tipo})")
        except pyodbc.Error as e:
            msg = self._mensaje_error_sql(e)
            self._registrar(quien, email, herramienta, inicio, error=msg)
            return f"ERROR: {msg}"
        finally:
            conn.close()
        self._registrar(quien, email, herramienta, inicio, filas=len(objetos))
        lineas = [f"## Vistas de {self.nombre} (úsalas sin esquema)"]
        lineas += [f"- {objeto}: " + ", ".join(columnas) for objeto, columnas in objetos.items()]
        if not objetos:
            lineas.append("(el usuario de la base no ve ninguna vista: avisa al administrador)")
        if alcance:
            lineas.append(f"\nAlcance de este usuario: {alcance}")
        return "\n".join(lineas)


# ---------------------------------------------------------------------------
# Cierre (base Presupuesto, el mismo modelo del conector "Cierre SQL" de Claude)
# ---------------------------------------------------------------------------
DESCRIPCION_CIERRE = (
    "Consulta el modelo de CIERRE de los fundos (Softland, base Presupuesto; el mismo del conector «Cierre SQL» "
    "de Claude): gastos (presupuesto y real), centros de costo, hectáreas, mano de obra, ingresos, consolidado y "
    "proyección de cierre, márgenes EERR de los cierres anuales y el detalle de gastos, facturas y consumos. "
    "Cifras en CLP salvo las columnas *_USD. Es OTRA base que la de cosecha y packing: no mezcles sus cifras sin "
    "decirlo.\n"
    "- Los datos ya vienen filtrados por los grupos de fundos que el usuario tiene autorizados (los muestra "
    "ver_modelo_cierre). Si pide un grupo que no ve, el resultado sale vacío: díselo en vez de inventar.\n"
    "- T-SQL de SQL Server 2019 con nivel de compatibilidad 100: sin STRING_AGG ni TRY_CAST (usa FOR XML PATH y CAST).\n"
    "- Responde breve (se lee en WhatsApp): cifras grandes en millones con 1 decimal y separador de miles con "
    "punto; di el corte usado (mes) y si es presupuesto, real o ambos.\n"
    "- Si piden estos datos en PDF o Excel, ármalo con generar_informe usando fuente='cierre' en cada sección (y "
    "fuente_kpis='cierre' para las cifras clave): las consultas son las mismas que aquí.\n"
    "- El informe o la presentación para el Directorio no se arma con esta herramienta: si lo piden, explica que "
    "por ahora se prepara en Claude con el conector Cierre SQL."
)
REGLAS_CIERRE_NO_DISPONIBLES = (
    "(No se pudieron leer las reglas del modelo desde el conector Cierre SQL: antes de dar cifras de ingresos, "
    "temporadas o EERR, advierte que las reglas de negocio del modelo no están disponibles.)"
)


class BaseCierre(BaseExterna):
    """Modelo Cierre_* de la base Presupuesto. El login claude_movil solo lee el esquema movil, cuyas
    vistas filtran por el correo que se fija en la sesión. Las reglas de negocio se leen del conector
    (REGLAS_MODELO de C:\\ClaudeCierre\\server.py, o CIERRE_REGLAS_ARCHIVO) al arrancar el bot: si
    cambian allá, basta reiniciar el bot."""

    requiere_email = True
    nota_informe = ("Cierre según la base Presupuesto (Softland): muestra solo los grupos de fundos autorizados para el "
                    "correo del usuario. Cifras en CLP salvo las columnas en USD.")

    def __init__(self):
        ruta = _env("CIERRE_REGLAS_ARCHIVO", r"C:\ClaudeCierre\server.py")
        reglas = _texto_de_archivo(ruta, "REGLAS_MODELO") or REGLAS_CIERRE_NO_DISPONIBLES
        super().__init__(
            id="cierre", nombre="Cierre (Presupuesto)", prefijo_env="CIERRE",
            descripcion=DESCRIPCION_CIERRE, reglas=reglas, esquema="movil",
            base_defecto="Presupuesto", usuario_defecto="claude_movil",
        )

    def _preparar_sesion(self, cur, email):
        cur.execute("SET NOCOUNT ON; SET LANGUAGE Spanish; SET LOCK_TIMEOUT 30000;")
        # @read_only = 1: nada de lo que corra después en esta sesión puede cambiar el correo.
        cur.execute("EXEC sys.sp_set_session_context @key = N'email', @value = ?, @read_only = 1;", email)
        cur.execute("SELECT Grupo FROM movil.MiAcceso ORDER BY Grupo;")
        grupos = [fila[0] for fila in cur.fetchall()]
        if not grupos:
            logger.warning(f"cierre: el correo {email} no tiene acceso en dbo.Cierre_UsuarioAcceso")
            raise ErrorBase(f"El correo {email} no tiene acceso a los datos de Cierre. Pide al administrador que "
                            "lo agregue en dbo.Cierre_UsuarioAcceso.")
        return "todos los grupos" if "*" in grupos else ", ".join(grupos)


# ---------------------------------------------------------------------------
# Registro de bases. Para sumar una base: agregarla aquí y poner sus variables en el .env.
# ---------------------------------------------------------------------------
BASES = {b.id: b for b in (
    BaseCierre(),
)}


def base_de_herramienta(nombre):
    """La base dueña de esa herramienta, o None si no es de una base externa."""
    return next((b for b in BASES.values() if nombre in (b.herramienta_consulta, b.herramienta_modelo)), None)


def herramientas(ids):
    """Definiciones para Claude de las bases `ids` que están configuradas, en orden fijo (caché)."""
    return [t for id_, b in BASES.items() if id_ in ids and b.configurada for t in b.herramientas()]


for _base in BASES.values():
    (logger.info if _base.configurada else logger.warning)(f"Base externa '{_base.id}' ({_base.nombre}): {_base.estado()}")
