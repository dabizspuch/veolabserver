import pymysql
import base64
import json
import logging
import time
from .database_config import DatabaseConfig
from datetime import datetime
from collections import namedtuple

class DatabaseVeolab (object):
    """
    Esta clase permite conectarse a la base de datos de Veolab y realizar
    actualizaciones y consultas concretas necesarias para sincronizar con IGEO. 
    """

    def __init__(self, connection=None, cursor=None, serial=None, division=None):
        self.connection = connection  
        self.cursor = cursor         
        self.serial = serial  # Serie
        self.division = division  # Delegación
        self._col_cache = {}  # Caché de existencia de columnas opcionales (OPECJSO, INFCJSO...)

    def open(self):
        # Conecta a la base de datos, prepara el cursor y carga la configuración
        try:
            # Conecta a MySQL
            db_config = DatabaseConfig()
            db_config.read_config()
            self.connection = pymysql.connect(host=db_config.host,
                                        port=int(db_config.port), 
                                        user=db_config.user,
                                        password=db_config.passwd,
                                        database=db_config.database,
                                        cursorclass=pymysql.cursors.DictCursor,
                                        connect_timeout=20)
            # Crea el cursor
            self.cursor = self.connection.cursor()
            # Lee la configuración de serie y delegación
            self.refresh_serial()

        except pymysql.Error as e:
            if self.connection is not None:
                self.logdb("ERROR", "Error al establecer la conexión con la base de datos:", e, True)
            else:
                print ("Error al establecer la conexión con la base de datos:", e)

    def close(self):
        # Desconecta la base de datos
        try:
            if self.connection is not None:
                self.connection.close()
        except pymysql.Error:
            pass

    def ensure_connection(self):
        try:
            if self.connection is None:
                raise pymysql.Error("Conexión no inicializada")
            self.connection.ping(reconnect=True)
        except pymysql.Error as e:
            logging.warning(f"Conexión perdida. Reintentando... {e}")
            self.open()

    def column_exists(self, table, column):
        # Comprueba (cacheado) si una columna existe. Sirve para campos opcionales
        # que el usuario puede haber añadido en Veolab (p.ej. OPECJSO, INFCJSO) o no.
        key = (table, column)
        if key not in self._col_cache:
            try:
                self.cursor.execute(
                    "SELECT 1 FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s LIMIT 1",
                    (table, column)
                )
                self._col_cache[key] = self.cursor.fetchone() is not None
            except pymysql.Error as e:
                logging.warning(f"No se pudo comprobar la existencia de {table}.{column}: {e}")
                return False
        return self._col_cache[key]

    def refresh_serial(self):
        # Resuelve la delegación y la serie a usar para operaciones:
        #   1) PARCIGS (serie configurada para IGEO en ACCPAR), si está informada.
        #   2) Si no, la serie predeterminada de LABOPE (CLTBPRE='T'), igual que la
        #      función DBS_ObtenSeriePredeterminada de Veolab.
        # Se relee en cada alta para no depender del reinicio del servicio cuando
        # cambia la serie predeterminada en Veolab.
        self.cursor.execute("SELECT PARCIGS, PARCIGD FROM ACCPAR WHERE PAR1COD = 1")
        row = self.cursor.fetchone()
        if row is None:
            return
        self.division = row['PARCIGD'] if row['PARCIGD'] is not None else ""
        self.serial = row['PARCIGS'] if row['PARCIGS'] is not None else ""
        if self.serial == "":
            self.cursor.execute(
                "SELECT CLTCSER FROM ACCCLT WHERE DEL3COD = %s AND CLTCTAB = 'LABOPE' AND CLTBPRE = 'T'",
                (self.division, )
            )
            row = self.cursor.fetchone()
            if row is not None:
                self.serial = row['CLTCSER'] if row['CLTCSER'] is not None else ""

    def get_rabbit_config(self):
        query = "SELECT PARCIGI, PARCIGP, PARCIGV, PARCIGU, PARCIGC, PARNSEC FROM ACCPAR WHERE PAR1COD = 1;"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        return row

    def get_technical_key(self, table_name):
        # Obtiene la clave técnica para tabla de entrada
        query = """
            SELECT CLTNVAL FROM ACCCLT 
            WHERE DEL3COD = %s AND CLTCTAB = %s AND CLTCSER = %s 
            FOR UPDATE
        """
        self.cursor.execute(query, (self.division, table_name, self.serial))
        row = self.cursor.fetchone()
        
        if row is None:
            next_key = 1
            query = """
                INSERT INTO ACCCLT (CLTNVAL, DEL3COD, CLTCTAB, CLTCSER) 
                VALUES (%s, %s, %s, %s)
            """
        else:
            max_key = row['CLTNVAL']
            next_key = max_key + 1
            query = """
                UPDATE ACCCLT SET CLTNVAL = %s 
                WHERE DEL3COD = %s AND CLTCTAB = %s AND CLTCSER = %s
            """

        val = (next_key, self.division, table_name, self.serial)
        self.cursor.execute(query, val)
        self.connection.commit()
        return next_key 

    def next_igelog_key(self):
        # IGELOG tiene PK (DEL3COD, LOG1COD), SIN serie, y solo lo escribe este servicio.
        # Por eso el contador va por delegación (no por serie, a diferencia de get_technical_key)
        # y se sincroniza con el máximo real para no colisionar tras restauraciones, cambios de
        # serie o limpiezas del log.
        self.cursor.execute(
            "SELECT CLTNVAL FROM ACCCLT WHERE DEL3COD = %s AND CLTCTAB = 'IGELOG' AND CLTCSER = '' FOR UPDATE",
            (self.division,)
        )
        row = self.cursor.fetchone()
        self.cursor.execute(
            "SELECT COALESCE(MAX(LOG1COD), 0) AS maximo FROM IGELOG WHERE DEL3COD = %s",
            (self.division,)
        )
        max_real = self.cursor.fetchone()['maximo']
        contador = row['CLTNVAL'] if row is not None else 0
        next_key = max(contador, max_real) + 1
        if row is None:
            self.cursor.execute(
                "INSERT INTO ACCCLT (CLTNVAL, DEL3COD, CLTCTAB, CLTCSER) VALUES (%s, %s, 'IGELOG', '')",
                (next_key, self.division)
            )
        else:
            self.cursor.execute(
                "UPDATE ACCCLT SET CLTNVAL = %s WHERE DEL3COD = %s AND CLTCTAB = 'IGELOG' AND CLTCSER = ''",
                (next_key, self.division)
            )
        self.connection.commit()
        return next_key

    def logdb(self, command, text, details, commit=False):
        val = None
        try:
            cod = self.next_igelog_key()
            query = """
                INSERT INTO IGELOG (DEL3COD, LOG1COD, LOGTFEC, LOGCTIP, LOGCDES, LOGCDET) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            dateReg = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            str_details = str(details).replace("\n", "").replace("\t", "")
            val = (self.division, cod, dateReg, command, text, str_details)
            self.cursor.execute(query, val)
            if commit:
                self.connection.commit()

            # Logging con nivel según el tipo de comando
            fecha = time.strftime('%d/%m/%y')
            mensaje = f"{text} {fecha} - {details}"
            if command == "ERROR":
                logging.error(mensaje)
            elif command == "WARNING":
                logging.warning(mensaje)
            else:
                logging.info(mensaje)

        except pymysql.Error as e:
            import traceback
            logging.error(f"Error al registrar en log de base de datos: {e}")
            logging.error(f"Intento de insertar: {val}")
            logging.error(traceback.format_exc())            


    def get_client(self, client_igeo):
        # Obtiene el código del cliente según Veolab
        query = "SELECT DEL3COD, CLI1COD FROM SINCLI WHERE CLICIGC = %s"
        self.cursor.execute(query, (client_igeo, ))        
        row = self.cursor.fetchone()
        if row is not None:
            return row['DEL3COD'], row['CLI1COD']
        else:
            return "", ""

    def get_service(self, service_igeo, div_client, cod_client):
        # Obtiene datos del servicio buscando por el mapeo de cliente
        query = """
            SELECT LABSER.DEL3COD, LABSER.SER1COD, LABSER.SERNPRE, LABSER.SERCDTO, LABSER.TIO2DEL, LABSER.TIO2COD, LABSER.MAT2DEL, LABSER.MAT2COD 
            FROM LABSYC 
            LEFT JOIN LABSER ON (LABSYC.SER3DEL = LABSER.DEL3COD AND LABSYC.SER3COD = LABSER.SER1COD)
            WHERE LABSYC.SYCCREF = %s AND LABSYC.CLI3DEL = %s AND LABSYC.CLI3COD = %s
        """
        self.cursor.execute(query, (service_igeo, div_client, cod_client))        
        row = self.cursor.fetchone()
        if row is not None:
            return (
                row['DEL3COD'], 
                row['SER1COD'], 
                row['SERNPRE'], 
                row['SERCDTO'], 
                row['TIO2DEL'], 
                row['TIO2COD'], 
                row['MAT2DEL'], 
                row['MAT2COD']
            )
        else:
            return ("", "", 0, "", "", 0, "", 0)

    def get_parameter(self, parameter_igeo, div_client, cod_client):
        # Obtiene datos de la técnica buscando por el id de IGEO
        query = """
            SELECT LABTEC.DEL3COD, LABTEC.TEC1COD, LABTEC.TECCNOM, LABTEC.TECCNOI, LABTEC.TECBCUR, LABTEC.TECDACR, LABTEC.TECCPAR, 
                LABTEC.TECCABR, LABTEC.TECCCAS, LABTEC.TECNPRE, LABTEC.TECCDTO, LABTEC.TECCUNI, LABTEC.TECCLEY, LABTEC.TECCMET, 
                LABTEC.TECCMEA, LABTEC.TECCNOR, LABTEC.TECNTIE, LABTEC.TECCLIM, LABTEC.TECCMIN, LABTEC.TECCINC, LABTEC.TECCINS, 
                LABTEC.TECBEXP, LABTEC.SEC2DEL, LABTEC.SEC2COD 
            FROM LABTYC
            LEFT JOIN LABTEC ON (LABTYC.TEC3DEL = LABTEC.DEL3COD AND LABTYC.TEC3COD = LABTEC.TEC1COD)
            WHERE FIND_IN_SET(%s, LABTYC.TYCCREF) > 0
                AND LABTYC.CLI3DEL = %s 
                AND LABTYC.CLI3COD = %s            
        """
        self.cursor.execute(query, (parameter_igeo, div_client, cod_client))
        row = self.cursor.fetchone()
        return row

    def get_parameters_op(self, division, serial, code_op):
        # Obtiene la lista de técnicas de la operación de entrada
        query = """
            SELECT RESCNOM, RESCREF, RESCMET, RESCMIN, CORCVAL, RESCUNI 
            FROM LABRES 
            LEFT JOIN LABCOR ON (LABRES.OPE3DEL = LABCOR.OPE3DEL 
                AND LABRES.OPE3SER = LABCOR.OPE3SER 
                AND LABRES.OPE3COD = LABCOR.OPE3COD 
                AND LABRES.TEC3DEL = LABCOR.TEC3DEL 
                AND LABRES.TEC3COD = LABCOR.TEC3COD)
            WHERE LABRES.OPE3DEL = %s AND LABRES.OPE3SER = %s AND LABRES.OPE3COD = %s AND COR1COD = 1
        """
        self.cursor.execute(query, (division, serial, code_op))
        rows = self.cursor.fetchall()
        return rows

    def get_analyst(self, division, code):
        # Obtiene el código del primer analista asignado a la técnica
        query = "SELECT EMP3DEL, EMP3COD FROM LABTYE WHERE TEC3DEL = %s AND TEC3COD = %s"
        self.cursor.execute(query, (division, code))
        row = self.cursor.fetchone()
        return row

    def get_breakdown_type(self):
        # Obtiene el tipo de desglose configurado en Veolab
        query = "SELECT CONCTID FROM LABCON WHERE CON1COD = 1"
        self.cursor.execute(query)        
        row = self.cursor.fetchone()
        if row is not None:
            return row['CONCTID']
        else:
            return ""        

    def get_operation(self, reference_op):
        # Obtiene la clave completa de la primera operación con la referencia indicada
        if reference_op != "" and reference_op is not None:
            query = """
                SELECT DEL3COD, OPE1SER, OPE1COD FROM LABOPE 
                WHERE (OPECIGE = 'R' OR OPECIGE = 'E') AND OPECREF = %s
            """
            self.cursor.execute(query, (reference_op, ))
            row = self.cursor.fetchone()
            return row
        else:
            return None

    def get_document_name(self, division, serial, code_inf):
        # Obtiene el nombre del documento del informe de entrada
        query = "SELECT FATCNOM FROM DOCFAT WHERE DEL3COD = %s AND INF2SER = %s AND INF2COD = %s"
        self.cursor.execute(query, (division, serial, code_inf))
        row = self.cursor.fetchone()
        if row is not None:
            return row['FATCNOM']
        else:
            return None

    def get_document_pdf(self, division, serial, code_inf):
        # Obtiene el contenido en PDF en base 64 del documento del informe 
        query = """
            SELECT BLOLCON, BLONTAM FROM DOCBLO 
                LEFT JOIN DOCFAT ON (DOCBLO.DEL3COD = DOCFAT.DEL3COD 
                    AND DOCBLO.FAT3COD = DOCFAT.FAT1COD 
                    AND DOCBLO.VER3COD = DOCFAT.VER2COD)
            WHERE DOCFAT.DEL3COD = %s AND DOCFAT.INF2SER = %s AND DOCFAT.INF2COD = %s
            ORDER BY DOCBLO.DEL3COD, DOCBLO.BLO1COD
        """
        self.cursor.execute(query, (division, serial, code_inf))
        rows = self.cursor.fetchall()
        blob = b""
        for row in rows:
            chunk = row['BLOLCON']
            size = row['BLONTAM']
            blob += chunk[:size]  
        
        return base64.b64encode(blob).decode('utf8')

    def get_reports(self):
        # Obtiene la estructura exacta para enviar el informe a la cola de IGEO
        query = """
            SELECT DISTINCT LABOPE.DEL3COD AS OPE1DEL, OPE1COD, OPE1SER, OPECREF, OPECDES, 
                OPEDREG, OPETREC, OPECOBS, LABOPE.CLI2DEL, LABOPE.CLI2COD, OPECTEM, OPECENV, 
                OPECLUR, OPECCAN, OPECREC, OPECTIP, OPENPRE, OPECDTO, OPECTEC, OPEBFAB, OPECTID, 
                LABOPE.TIO2DEL, LABOPE.TIO2COD, LABOPE.MAT2DEL, LABOPE.MAT2COD, OPEDINI, OPEDFIN, 
                OPECIDG, SINCLI.CLICIGC, SINCLI.CLICCIG, LABSER.SERCNOM, LABSYC.SYCCREF, 
                LABINF.DEL3COD AS INF1DEL, INF1SER, INF1COD 
            FROM LABOPE 
            LEFT JOIN SINCLI ON (LABOPE.CLI2DEL = SINCLI.DEL3COD
                AND LABOPE.CLI2COD = SINCLI.CLI1COD)
            LEFT JOIN LABIYO ON (LABOPE.DEL3COD = LABIYO.OPE3DEL 
                AND LABOPE.OPE1SER = LABIYO.OPE3SER 
                AND LABOPE.OPE1COD = LABIYO.OPE3COD)
            LEFT JOIN LABINF ON (LABIYO.INF3DEL = LABINF.DEL3COD 
                AND LABIYO.INF3SER = LABINF.INF1SER 
                AND LABIYO.INF3COD = LABINF.INF1COD) 
            LEFT JOIN LABOYS ON (LABOPE.DEL3COD = LABOYS.OPE3DEL
                AND LABOPE.OPE1SER = LABOYS.OPE3SER
                AND LABOPE.OPE1COD = LABOYS.OPE3COD)
            LEFT JOIN LABSER ON (LABOYS.SER3DEL = LABSER.DEL3COD
                AND LABOYS.SER3COD = LABSER.SER1COD)
            LEFT JOIN LABSYC ON (LABSER.DEL3COD = LABSYC.SER3DEL 
                AND LABSER.SER1COD = LABSYC.SER3COD 
                AND LABOPE.CLI2DEL = LABSYC.CLI3DEL 
                AND LABOPE.CLI2COD = LABSYC.CLI3COD)
            WHERE LABOPE.OPECIGE = 'R' AND LABINF.INFDENV IS NOT NULL
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        reports = []
        for row in rows:
            try:
                reports.append(self.build_report(row))
            except Exception as e:
                logging.error(
                    f"Error al construir el informe de la operación {row.get('OPECREF')}: {e}. "
                    f"Se omite esa muestra y se continúa con el resto."
                )
        return reports

    def build_report(self, row):
        # Construye el dict de un informe a partir de una fila de get_reports.
        # Aislado para que un fallo en una muestra (p.ej. fecha nula) no tumbe todo el lote.
        def fmt_dt(value):
            return value.strftime('%d/%m/%Y %H:%M:%S') if value else None

        def to_int(value):
            # IGEO espera empresaId numérico, pero CLICIGC se guarda como varchar.
            if value is None or str(value).strip() == "":
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                logging.warning(f"empresaId no numérico ({value!r}) en la operación {row['OPECREF']}; se envía tal cual")
                return value

        report = {}
        report['tipoEntidadIgeo'] = "ANALITICA"
        report['idEntidadIgeo'] = row['OPECIDG']
        report['codigoEntidadIgeo'] = row['OPECREF']
        report['comando'] = "UPDATE"
        report['fecha'] = datetime.now().date().strftime('%d/%m/%Y %H:%M:%S')

        report['datos'] = {}
        report['datos']['id'] = row['OPECIDG']
        report['datos']['codigoMuestra'] = row['OPECREF']
        report['datos']['muestra'] = row['OPECDES']
        report['datos']['fechaCreacion'] = fmt_dt(row['OPETREC'])
        report['datos']['observaciones'] = row['OPECOBS']
        report['datos']['fechaInicioMuestra'] = fmt_dt(row['OPEDINI'])
        report['datos']['fechaFinMuestra'] = fmt_dt(row['OPEDFIN'])
        report['datos']['lugarRecogidaMuestra'] = row['OPECLUR']
        report['datos']['temperatura'] = row['OPECTEM']
        report['datos']['tipoEnvase'] = row['OPECENV']
        report['datos']['codigoGrupoObjetoAnalisis'] = row['SYCCREF']
        report['datos']['grupoObjetoAnalisis'] = row['SERCNOM']
        report['datos']['volumenMuestra'] = row['OPECCAN']
        report['datos']['transportista'] = row['OPECREC']

        report['datos']['objetosAnalisis'] = []
        tec_rows = self.get_parameters_op(row['OPE1DEL'], row['OPE1SER'], row['OPE1COD'])
        for tec_row in tec_rows:
            objeto_analisis = {
                'objetoAnalisis': tec_row['RESCNOM'],
                'codigoObjetoAnalisis': tec_row['RESCREF'],
                'metodo': tec_row['RESCMET'],
                'minimo': tec_row['RESCMIN'],
                'resultado': tec_row['CORCVAL'],
                'unidadDeMedida': tec_row['RESCUNI']
            }
            report['datos']['objetosAnalisis'].append(objeto_analisis)

        report['datos']['nombreDocumento'] = self.get_document_name(row['INF1DEL'], row['INF1SER'], row['INF1COD'])
        report['datos']['pdfAnalitica'] = self.get_document_pdf(row['INF1DEL'], row['INF1SER'], row['INF1COD'])
        report['empresaId'] = to_int(row['CLICIGC'])

        # Autodefinibles
        query_aut = """
            SELECT AUT3DEL, AUT3COD, OYACVAL, AUTCNOM FROM LABOYA
                LEFT JOIN LABAUT ON (LABOYA.AUT3DEL = LABAUT.DEL3COD AND LABOYA.AUT3COD = LABAUT.AUT1COD)
                WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s
        """
        self.cursor.execute(query_aut, (row['OPE1DEL'], row['OPE1SER'], row['OPE1COD']))
        rows_selfdefining = self.cursor.fetchall()

        for row_selfdefining in rows_selfdefining:
            if row_selfdefining['AUTCNOM'] is not None:
                field_selfdefining = self.get_field_selfdefining(row_selfdefining['AUTCNOM'])
                report['datos'][field_selfdefining] = row_selfdefining['OYACVAL']

        # Guarda el JSON que se envía a IGEO (sin el PDF) si el usuario ha añadido
        # la columna INFCJSO en LABINF (opcional). En este punto report aún no lleva
        # la clave 'cola', así que coincide con el mensaje que se envía a la cola.
        if row['INF1COD'] is not None and self.column_exists('LABINF', 'INFCJSO'):
            try:
                envio = dict(report)
                envio['datos'] = {k: v for k, v in report['datos'].items() if k != 'pdfAnalitica'}
                json_envio = json.dumps(envio, ensure_ascii=False)
                self.cursor.execute(
                    "UPDATE LABINF SET INFCJSO = %s WHERE DEL3COD = %s AND INF1SER = %s AND INF1COD = %s",
                    (json_envio, row['INF1DEL'], row['INF1SER'], row['INF1COD'])
                )
                self.connection.commit()
            except Exception as e:
                logging.warning(f"No se pudo guardar INFCJSO de la operación {row['OPECREF']}: {e}")

        report['cola'] = row['CLICCIG']
        return report

    def mark_sample_sent(self, reference_op):
        # Actualiza el estado de la operación a enviada a IGEO
        query = "UPDATE LABOPE SET OPECIGE = 'E' WHERE OPECIGE = 'R' AND OPECREF = %s"
        self.cursor.execute(query, (reference_op, ))    

    def mark_sample_report(self, reference_op):
        # Actualiza el estado de la operación a informe correctamente recibido por IGEO
        query = "UPDATE LABOPE SET OPECIGE = 'I' WHERE OPECIGE = 'E' AND OPECREF = %s"
        self.cursor.execute(query, (reference_op, ))    

    def get_selfdefining(self, field):
        # Obtiene el autodefinible de Veolab para el campo de entrada que corresponda con la nomenclatura
        import re
        humanName = re.sub(r'([A-Z])', r' \1', field).capitalize()
        query = "SELECT DEL3COD, AUT1COD FROM LABAUT WHERE AUTCNOM = %s"        
        self.cursor.execute(query, (humanName, ))
        row = self.cursor.fetchone()
        if row is not None:
            SelfDefining = namedtuple('SelfDefining', ['division', 'code'])
            return SelfDefining(row['DEL3COD'], row['AUT1COD'])
        return None
    
    def get_field_selfdefining(self, human_name):
        parts = human_name.split()
        field = parts[0].lower() + ''.join(part.capitalize() for part in parts[1:])
        return field
    
    def iter_fields_with_subgroup(self, payload, subgroup_key):
        for field, value in payload.items():
            if field == subgroup_key and isinstance(value, dict):
                for subfield, subvalue in value.items():
                    yield subfield, subvalue
            else:
                yield field, value

    def script_create_sample (self, payload, client_id, igeo_id, raw_json=None):
        div_client, cod_client = self.get_client(client_id)

        group_code = payload.get("codigoGrupoObjetoAnalisis")

        if not group_code:
            objetos = payload.get("objetosAnalisis") or []
            if objetos:
                group_code = objetos[0].get("codigoGrupoObjetoAnalisis")

        if not group_code:
            raise ValueError("codigoGrupoObjetoAnalisis no informado en payload")
            
        (
            div_service, 
            cod_service, 
            prize, 
            discount, 
            div_op_type, 
            cod_op_type, 
            div_matrix, 
            cod_matrix
        ) = self.get_service(group_code, div_client, cod_client)

        breakdown_type = self.get_breakdown_type()
        id_op = self.get_technical_key('LABOPE')    

        # Tabla LABRES (parámetros)
        array_val = []
        array_parameters = []
        array_employes = []
        array_sections = []
        array_cor = []
        # LABRES: columnas fijas + RESCOBS opcional (observaciones del objeto de análisis).
        labres_columns = [
            "OPE3DEL", "OPE3SER", "OPE3COD", "TEC3DEL", "TEC3COD", "RESCNOM",
            "RESCNOI", "RESBCUR", "RESDACR", "RESCPAR", "RESCABR", "RESCCAS", "RESNPRE", "RESCDTO",
            "RESCUNI", "RESCLEY", "RESCMET", "RESCMEA", "RESCNOR", "RESNTIE", "RESCLIM", "RESCMIN",
            "RESCINC", "RESCINS", "RESBEXP", "SEC2DEL", "SEC2COD", "RESNORD", "EMP2DEL", "EMP2COD",
            "SER2DEL", "SER2COD", "RESCREF"
        ]
        has_rescobs = self.column_exists('LABRES', 'RESCOBS')
        if has_rescobs:
            labres_columns.append("RESCOBS")
        labres_query = (
            "INSERT INTO LABRES (" + ", ".join(labres_columns) + ") "
            "VALUES (" + ", ".join(["%s"] * len(labres_columns)) + ")"
        )
        for index, igeo_parameter in enumerate(payload['objetosAnalisis']):
            tec_fields = self.get_parameter(igeo_parameter['codigoObjetoAnalisis'], div_client, cod_client)
            if tec_fields is not None:
                analyst = self.get_analyst(tec_fields['DEL3COD'], tec_fields['TEC1COD'])
                if analyst is not None:
                    div_analyst = analyst['EMP3DEL']
                    cod_analyst = analyst['EMP3COD']
                else:
                    div_analyst = ""
                    cod_analyst = 0

                val = [
                    self.division,
                    self.serial,
                    id_op,
                    *tec_fields.values(),
                    index,
                    div_analyst,
                    cod_analyst,
                    div_service,
                    cod_service,
                    igeo_parameter['codigoObjetoAnalisis']
                ]
                # Vuelca las observaciones del objeto de análisis si existe RESCOBS.
                if has_rescobs:
                    val.append(igeo_parameter.get('observaciones'))
                array_val.append(val)
                # Nombres de técnicas para OPECTEC
                array_parameters.append(tec_fields['TECCNOM']) 
                # Vector de secciones para generar LABOYD
                tuple_section = (tec_fields['SEC2DEL'], tec_fields['SEC2COD'])
                if tuple_section not in array_sections:
                    array_sections.append (tuple_section)  
                # Vector de analistas para generar LABOYE
                tuple_analyst = (div_analyst, cod_analyst)
                if tuple_analyst not in array_employes and tuple_analyst[1] != 0:                        
                    array_employes.append(tuple_analyst)      
                # Vector para generar LABCOR
                array_cor.append ((self.division, self.serial, id_op, tec_fields['DEL3COD'], tec_fields['TEC1COD']))

        if len(array_val) > 0:
            self.cursor.executemany(labres_query, array_val)

        # Tabla LABCOR (columnas)
        query = """
            INSERT INTO LABCOR (OPE3DEL, OPE3SER, OPE3COD, TEC3DEL, TEC3COD, COR1COD, CORCTIT, 
                CORCTI2, CORCTI3, CORBINF, CORBRES, CORBEDI, CORBACT)
            SELECT %s, %s, %s, TEC3DEL, TEC3COD, COT1COD, COTCTIT, 
                COTCTI2, COTCTI3, COTBINF, COTBRES, COTBEDI, COTBACT FROM LABCOT                
            WHERE TEC3DEL = %s AND TEC3COD = %s
        """
        self.cursor.executemany(query, array_cor)

        # Tabla LABOPE (operaciones)
        columns = [
            "DEL3COD", "OPE1SER", "OPE1COD", "OPECREF", "OPECDES",
            "OPEDREG", "OPETREC", "OPECOBS", "CLI2DEL", "CLI2COD", "OPECTEM", "OPECENV", "OPECLUR",
            "OPECCAN", "OPECREC", "OPECTIP", "OPENPRE", "OPECDTO", "OPECTEC", "OPEBFAB", "OPECTID",
            "TIO2DEL", "TIO2COD", "MAT2DEL", "MAT2COD", "OPECIDG", "OPECIGE"
        ]
        val = [
            self.division,
            self.serial,
            id_op,
            payload['codigoMuestra'],
            payload['muestra'],
            datetime.now().date(),
            datetime.strptime(payload['fechaCreacion'], '%d/%m/%Y %H:%M:%S'),
            payload['observaciones'],
            div_client,
            cod_client,
            payload['temperatura'],
            payload['tipoEnvase'],
            payload['lugarRecogidaMuestra'],
            payload['volumenMuestra'],
            payload['transportista'],
            "E",
            prize,
            discount,
            ", ".join(array_parameters),
            "T",
            breakdown_type,
            div_op_type,
            cod_op_type,
            div_matrix,
            cod_matrix,
            igeo_id,
            "R"
        ]
        # Guarda el JSON recibido si el usuario ha añadido la columna OPECJSO (opcional).
        if raw_json is not None and self.column_exists('LABOPE', 'OPECJSO'):
            columns.append("OPECJSO")
            val.append(raw_json)
        placeholders = ", ".join(["%s"] * len(val))
        query = f"INSERT INTO LABOPE ({', '.join(columns)}) VALUES ({placeholders})"
        self.cursor.execute(query, val)
        
        # Tabla LABOYA (valores de autodefinibles)
        query = """
            INSERT INTO LABOYA (OPE3DEL, OPE3SER, OPE3COD, AUT3DEL, AUT3COD) 
            VALUES (%s, %s, %s, %s, %s)
        """
        val = (self.division, self.serial, id_op, '', 0) # El autodefinible cero es obligatorio
        self.cursor.execute(query, val)

        for field, value in self.iter_fields_with_subgroup(payload, "otrosParametros"):
            selfdefining = self.get_selfdefining(field)
            if selfdefining is not None:
                # Inserta los campos autodefinibles
                query = """
                    INSERT INTO LABOYA (OPE3DEL, OPE3SER, OPE3COD, AUT3DEL, AUT3COD, OYACVAL) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                val = (self.division, self.serial, id_op, selfdefining.division, selfdefining.code, value)
                self.cursor.execute(query, val)

        # Tabla LABOYS (servicios)
        if cod_service is not None:
            query = """
                INSERT INTO LABOYS (OPE3DEL, OPE3SER, OPE3COD, SER3DEL, SER3COD, OYSNPRE, OYSCDTO, OYSNPOS, OYSBPRE) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'T')
            """
            val = (self.division, self.serial, id_op, div_service, cod_service, prize, discount, 1)
            self.cursor.execute(query, val)
        
        # Tabla LABOYE (empleados)
        array_val.clear()
        query = """
            INSERT INTO LABOYE (OPE3DEL, OPE3SER, OPE3COD, EMP3DEL, EMP3COD) 
            VALUES (%s, %s, %s, %s, %s)
        """
        for employe in array_employes:
            array_val.append((self.division, self.serial, id_op, employe[0], employe[1]))        
        if len(array_val) > 0:
            self.cursor.executemany(query, array_val)

        # Tabla LABOYD (departamentos)            
        array_val.clear()
        for section in array_sections:
            query = "SELECT DISTINCT DEP2DEL, DEP2COD FROM LABSEC WHERE DEL3COD = %s AND SEC1COD = %s "
            self.cursor.execute(query, section)
            row = self.cursor.fetchone()
            if row is not None:            
                query = """
                    INSERT INTO LABOYD (OPE3DEL, OPE3SER, OPE3COD, DEP3DEL, DEP3COD) 
                    VALUES (%s, %s, %s, %s, %s)
                """
                array_val.append((self.division, self.serial, id_op, row['DEP2DEL'], row['DEP2COD']))
        
        self.cursor.executemany(query, array_val)

    def script_delete_sample(self, reference_op):
        # Crea todos los registros necesarios para crear una muestra en Veolab
        dic_val = self.get_operation (reference_op)        
        if dic_val is not None:
            val = list(dic_val.values())
            query = "DELETE FROM LABCOR WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s"
            self.cursor.execute(query, val)
            query = "DELETE FROM LABOYE WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s"
            self.cursor.execute(query, val)
            query = "DELETE FROM LABOYD WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s"
            self.cursor.execute(query, val)
            query = "DELETE FROM LABOYA WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s"
            self.cursor.execute(query, val)
            query = "DELETE FROM LABOYS WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s"
            self.cursor.execute(query, val)        
            query = "DELETE FROM LABRES WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s"
            self.cursor.execute(query, val)
            query = "DELETE FROM LABOPE WHERE DEL3COD = %s AND OPE1SER = %s AND OPE1COD = %s"
            self.cursor.execute(query, val)

    def sample_exists(self, reference_op, client_igeo):
        # Comprueba si ya existe una operación con esa referencia para el cliente.
        # Sirve para idempotencia: RabbitMQ puede reentregar (redelivered) un mensaje
        # no confirmado tras una reconexión, y no se debe crear la muestra dos veces.
        div_client, cod_client = self.get_client(client_igeo)
        query = "SELECT 1 FROM LABOPE WHERE OPECREF = %s AND CLI2DEL = %s AND CLI2COD = %s LIMIT 1"
        self.cursor.execute(query, (reference_op, div_client, cod_client))
        return self.cursor.fetchone() is not None

    def get_operation_full(self, reference_op, client_igeo):
        # Localiza la operación por referencia + cliente, con su estado operativo (OPENEST),
        # sin filtrar por estado. Devuelve dict (DEL3COD, OPE1SER, OPE1COD, OPENEST) o None.
        div_client, cod_client = self.get_client(client_igeo)
        query = """
            SELECT DEL3COD, OPE1SER, OPE1COD, OPENEST FROM LABOPE
            WHERE OPECREF = %s AND CLI2DEL = %s AND CLI2COD = %s
        """
        self.cursor.execute(query, (reference_op, div_client, cod_client))
        return self.cursor.fetchone()

    def create_sample(self, payload, client_id, igeo_id, raw_json=None):
        self.ensure_connection()
        # Relee la serie predeterminada vigente (puede haber cambiado sin reiniciar).
        self.refresh_serial()
        if self.sample_exists(payload['codigoMuestra'], client_id):
            self.logdb("WARNING", f"Alta duplicada ignorada (la muestra ya existe): {payload['codigoMuestra']}", "", True)
            return
        self.script_create_sample(payload, client_id, igeo_id, raw_json)
        self.logdb("CREATE", f"Muestra creada: {payload['codigoMuestra']}", "")
        self.connection.commit()

    def script_update_sample(self, payload, op, raw_json=None):
        # Actualiza SOLO la cabecera de la operación y rehace los autodefinibles.
        # No toca parámetros (LABRES/LABCOR) ni resultados del laboratorio.
        div, serial, code = op['DEL3COD'], op['OPE1SER'], op['OPE1COD']

        set_cols = ["OPECDES = %s", "OPECOBS = %s", "OPETREC = %s", "OPECTEM = %s",
                    "OPECENV = %s", "OPECLUR = %s", "OPECCAN = %s", "OPECREC = %s"]
        set_val = [
            payload['muestra'],
            payload['observaciones'],
            datetime.strptime(payload['fechaCreacion'], '%d/%m/%Y %H:%M:%S'),
            payload['temperatura'],
            payload['tipoEnvase'],
            payload['lugarRecogidaMuestra'],
            payload['volumenMuestra'],
            payload['transportista'],
        ]
        # Actualiza el JSON recibido si el usuario ha añadido la columna OPECJSO (opcional).
        if raw_json is not None and self.column_exists('LABOPE', 'OPECJSO'):
            set_cols.append("OPECJSO = %s")
            set_val.append(raw_json)
        query = (
            "UPDATE LABOPE SET " + ", ".join(set_cols) +
            " WHERE DEL3COD = %s AND OPE1SER = %s AND OPE1COD = %s"
        )
        val = tuple(set_val) + (div, serial, code)
        self.cursor.execute(query, val)

        # Los autodefinibles son valores del técnico (sin resultados de laboratorio),
        # así que se pueden rehacer a partir del payload.
        self.cursor.execute(
            "DELETE FROM LABOYA WHERE OPE3DEL = %s AND OPE3SER = %s AND OPE3COD = %s",
            (div, serial, code)
        )
        # El autodefinible cero es obligatorio
        self.cursor.execute(
            "INSERT INTO LABOYA (OPE3DEL, OPE3SER, OPE3COD, AUT3DEL, AUT3COD) VALUES (%s, %s, %s, %s, %s)",
            (div, serial, code, '', 0)
        )
        for field, value in self.iter_fields_with_subgroup(payload, "otrosParametros"):
            selfdefining = self.get_selfdefining(field)
            if selfdefining is not None:
                self.cursor.execute(
                    "INSERT INTO LABOYA (OPE3DEL, OPE3SER, OPE3COD, AUT3DEL, AUT3COD, OYACVAL) VALUES (%s, %s, %s, %s, %s, %s)",
                    (div, serial, code, selfdefining.division, selfdefining.code, value)
                )

    def update_sample(self, payload, client_id, igeo_id, raw_json=None):
        # Modifica una muestra existente EN SITIO: solo cabecera + autodefinibles, y solo
        # si está registrada (OPENEST=0). Si no existe, se da de alta. No borra ni recrea.
        self.ensure_connection()
        op = self.get_operation_full(payload['codigoMuestra'], client_id)
        if op is None:
            # No existía: se crea igualmente (alta), dejando aviso de que llegó como UPDATE.
            # Relee la serie predeterminada vigente para el alta.
            self.refresh_serial()
            self.logdb("WARNING", f"UPDATE de muestra inexistente; se crea como alta: {payload['codigoMuestra']}", "", True)
            self.script_create_sample(payload, client_id, igeo_id, raw_json)
            self.connection.commit()
            return
        try:
            registrada = int(op['OPENEST']) == 0
        except (TypeError, ValueError):
            registrada = False
        if not registrada:
            self.logdb("WARNING", f"UPDATE no aplicado: la muestra ya avanzó de estado y no admite cambios (OPENEST={op['OPENEST']}): {payload['codigoMuestra']}", "", True)
            return
        self.script_update_sample(payload, op, raw_json)
        self.logdb("UPDATE", f"Muestra actualizada: {payload['codigoMuestra']}", "")
        self.connection.commit()

    def delete_sample(self, payload):
        # Borra de la base de datos la muestra de entrada
        self.ensure_connection()
        self.script_delete_sample(payload['codigoMuestra'])
        self.logdb("DELETE", f"Muestra eliminada: {payload['codigoMuestra']}", "")
        self.connection.commit()