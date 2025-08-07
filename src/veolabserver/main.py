import pika
import json
import time
import signal
import os
import logging
import hashlib
from threading import Thread, Event
from .database.database_veolab import DatabaseVeolab
from pika.exceptions import AMQPConnectionError, IncompatibleProtocolError

log_dir = "/var/log/veolabserver"
if os.name == 'nt':  # Windows
    log_dir = "C:\\veolabserver\\logs"

os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "veolabserver.log")),
        logging.StreamHandler()
    ]
)

stop_event = Event()

def process_received(body, database):
    # Procesa mensajes recibidos en la cola de analíticasRecibidas
    try:
        logging.info(f"Mensaje recibido: {body.decode('utf-8')}")
        json_body = json.loads(body)
                
        payload = json_body['datos']
        client_id = json_body['empresaId']
        igeo_id = json_body['idEntidadIgeo']

        if json_body['comando'] == 'CREATE' or json_body['comando'] is None:
            database.create_sample(payload, client_id, igeo_id)
        elif json_body['comando'] == 'UPDATE':
            database.update_sample(payload, client_id, igeo_id)
        elif json_body['comando'] == 'DELETE':
            database.delete_sample(payload)

    except json.JSONDecodeError as e:
        database.logdb("ERROR", "Error al decodificar el cuerpo JSON:", e, True)
    except Exception as e:
        database.logdb("ERROR", "Error inesperado:", e, True)


def process_performed(body, database):
    # Procesa mensajes recibidos en la cola de resultadoAnaliticasRealizadas
    try:
        json_body = json.loads(body)
        if json_body['codigo'] == "1":  # Sin errores
            codeSample = json_body['mensajeEnviado']['datos']['codigoMuestra']
            database.mark_sample_report(codeSample)
            database.logdb("OK", json_body['mensaje'], codeSample, True)
        else:           
            database.logdb("ERROR", json_body['mensaje'], json_body['errores'], True)

    except json.JSONDecodeError as e:
        database.logdb("ERROR", "Error al decodificar el cuerpo JSON:", e, True)
    except Exception as e:
        database.logdb("ERROR", "Error inesperado:", e, True)


def process_reports(channel):    
    # Envía informes finalizados a la cola de analiticasRealizadas
    database = None
    try:
        database = DatabaseVeolab()
        database.open()

        if database.connection is not None:
            channel.confirm_delivery()

            reports = database.get_reports()
            if reports is not None:                
                for report in reports:
                    queue = report.get('cola') # or 'analiticasRealizadas'
                    report_copy = {k: v for k, v in report.items() if k != 'cola'}
                    report_json = json.dumps(report_copy, ensure_ascii=False)

                    if not channel.is_open:
                        logging.warning(f"Canal cerrado, no se puede enviar informe {report['codigoEntidadIgeo']}")
                        database.logdb("EXCEPTION", "Canal cerrado, no se pudo enviar informe", report['codigoEntidadIgeo'], True)
                        continue

                    for attempt in range(3):  # Hasta 3 intentos
                        try:
                            if not channel.is_open:
                                raise Exception("Canal cerrado")

                            channel.basic_publish(
                                exchange='analiticasRealizadas_exchange', 
                                routing_key=queue, 
                                body=report_json.encode('utf8'),
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            database.mark_sample_sent(report['codigoEntidadIgeo'])
                            database.logdb("OK", "Informe enviado", report['codigoEntidadIgeo'], True)
                            break  # Éxito, salir del bucle de reintentos
                        
                        except Exception as e:
                            logging.warning(f"Intento {attempt+1} fallido al enviar informe {report['codigoEntidadIgeo']}: {e}")
                            time.sleep(2)
                            if attempt == 2:  # último intento
                                database.logdb("EXCEPTION", f"Excepción al enviar informe {report['codigoEntidadIgeo']}: {str(e)}", report['codigoEntidadIgeo'], True)

        logging.info("Procesando informes ...")

    except Exception as e:
        logging.error(f"Error inesperado: {e}")

    finally:
        if database is not None:
            database.close()


def listener_receive(channel, database):
    # Escucha la cola analiticasRecibidas
    def callback(ch, method, properties, body):
        try:
            process_received(body, database)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error al procesar mensaje en analiticasRecibidas: {e}")            

    def on_cancel_callback(method_frame):
        logging.warning(f"Consumidor cancelado en analiticasRecibidas: {method_frame}")

    channel.basic_qos(prefetch_count=50)
    channel.basic_consume(queue='analiticasRecibidas', on_message_callback=callback, auto_ack=False)
    channel.add_on_cancel_callback(on_cancel_callback)

    logging.info("Esperando muestras ...")
    while not stop_event.is_set():
        try:
            channel.connection.process_data_events(time_limit=1)  # Reemplaza start_consuming
        except Exception as e:
            logging.error(f"Error en process_data_events: {e}")
            break


def listener_perform(channel, database):
    # Escucha la cola resultadoAnaliticasRealizadas
    def callback(ch, method, properties, body):
        try:
            process_performed(body, database)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error al procesar mensaje en resultadoAnaliticasRealizadas: {e}")   

    def on_cancel_callback(method_frame):
        logging.warning(f"Consumidor cancelado en resultadoAnaliticasRealizadas: {method_frame}")

    channel.basic_qos(prefetch_count=50)
    channel.basic_consume(queue='resultadoAnaliticasRealizadas', on_message_callback=callback, auto_ack=False)
    channel.add_on_cancel_callback(on_cancel_callback)
    
    logging.info("Esperando resultados ...")
    while not stop_event.is_set():
        try:
            channel.connection.process_data_events(time_limit=1)
        except Exception:
            break

def process_reports_loop(channel, seconds):
    while not stop_event.is_set():
        process_reports(channel)
        stop_event.wait(seconds)

def hash_config(config):
    config_string = ''.join([config.get(k, '') for k in ['PARCIGU', 'PARCIGC', 'PARCIGI', 'PACCIGP', 'PARCIGV']])
    return hashlib.sha256(config_string.encode()).hexdigest()

def monitor_config_changes(initial_hash):
    while not stop_event.is_set():
        try:
            db = DatabaseVeolab()
            db.open()
            if db.connection is not None:
                new_config = db.get_rabbit_config()
                new_hash = hash_config(new_config)
                if new_hash != initial_hash:
                    logging.info("Cambio detectado en configuración Rabbit. Reiniciando servicio...")
                    os._exit(1)
        except Exception as e:
            logging.error(f"Error al comprobar cambios en configuración: {e}")
        finally:
            db.close()
        time.sleep(60)

def is_valid_rabbit_config(config):
    required_keys = ['PARCIGU', 'PARCIGC', 'PARCIGI', 'PARCIGP', 'PARCIGV']
    return all(config.get(k) for k in required_keys)

def run():
    thread_receive = None
    thread_perform = None
    thread_report = None    
    database = None
    database_receive = None
    database_perform = None
    connection_reports = None
    connection_receive = None
    connection_perform = None

    try:
        database = DatabaseVeolab()
        database.open()
        rb_config = None
        if database.connection is not None:
            rb_config = database.get_rabbit_config()

        if rb_config and is_valid_rabbit_config(rb_config):
            credentials = pika.PlainCredentials(rb_config['PARCIGU'], rb_config['PARCIGC'])

            # Iniciar monitor de cambios de configuración
            initial_hash = hash_config(rb_config)
            Thread(target=monitor_config_changes, args=(initial_hash,), daemon=True).start()
            
            # Inicia el escuchador para la cola analiticasRecibidas 
            database_receive = DatabaseVeolab()
            database_receive.open()
            if database_receive.connection is not None:
                try:
                    connection_receive = pika.BlockingConnection(pika.ConnectionParameters(
                            host=rb_config['PARCIGI'],  
                            port=rb_config['PARCIGP'], 
                            virtual_host=rb_config['PARCIGV'],  
                            credentials=credentials,
                            heartbeat=60,
                            blocked_connection_timeout=300))
                except (AMQPConnectionError, IncompatibleProtocolError) as e:
                    logging.error(f"Error de conexión con RabbitMQ (receive): {e}")
                    time.sleep(3)
                    os._exit(1)                    
                channel_receive = connection_receive.channel()
                channel_receive.add_on_cancel_callback(lambda method_frame: logging.warning(f"Canal analiticasRecibidas cancelado: {method_frame}"))
                thread_receive = Thread(target=listener_receive, args=(channel_receive, database_receive))
                thread_receive.start()

            # Inicia el escuchador para la cola resultadoAnaliticasRealizadas
            database_perform = DatabaseVeolab()
            database_perform.open()
            if database_perform.connection is not None:
                try:
                    connection_perform = pika.BlockingConnection(pika.ConnectionParameters(
                            host=rb_config['PARCIGI'],  
                            port=rb_config['PARCIGP'],  
                            virtual_host=rb_config['PARCIGV'],  
                            credentials=credentials,
                            heartbeat=60,
                            blocked_connection_timeout=300))
                except (AMQPConnectionError, IncompatibleProtocolError) as e:
                    logging.error(f"Error de conexión con RabbitMQ (receive): {e}")
                    time.sleep(3)
                    os._exit(1)  
                channel_perform = connection_perform.channel()
                channel_receive.add_on_cancel_callback(lambda method_frame: logging.warning(f"Canal resultadoAnaliticasRealizadas cancelado: {method_frame}"))
                thread_perform = Thread(target=listener_perform, args=(channel_perform, database_perform))
                thread_perform.start()

            # Inicia una consulta periódica a la base de datos para procesar informes
            try:
                connection_reports = pika.BlockingConnection(pika.ConnectionParameters(
                        host=rb_config['PARCIGI'],  
                        port=rb_config['PARCIGP'],  
                        virtual_host=rb_config['PARCIGV'], 
                        credentials=credentials,
                        heartbeat=60,
                        blocked_connection_timeout=300))
            except (AMQPConnectionError, IncompatibleProtocolError) as e:
                logging.error(f"Error de conexión con RabbitMQ (receive): {e}")
                time.sleep(3)
                os._exit(1)  
            channel_reports = connection_reports.channel()
            seconds = int(rb_config['PARNSEC'] or 60) 
            if seconds <= 0:
                seconds = 60
            thread_report = Thread(target=process_reports_loop, args=(channel_reports, seconds))
            thread_report.start()                

            while not stop_event.is_set():
                time.sleep(1) 

            if channel_receive.is_open:
                channel_receive.stop_consuming()
            if channel_perform.is_open:
                channel_perform.stop_consuming()

            thread_receive.join()
            thread_perform.join()
            thread_report.join()
        else:
            logging.error("Configuración RabbitMQ incompleta o inválida. Reiniciando servicio...")
            time.sleep(3)
            os._exit(1)            

    except pika.exceptions.AMQPError as e:
        logging.error(f"EError de conexión RabbitMQ: {e}")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")

    finally:
        if connection_receive is not None and not connection_receive.is_closed:
            connection_receive.close()
        if connection_perform is not None and not connection_perform.is_closed:
            connection_perform.close()
        if connection_reports is not None and not connection_reports.is_closed:
            connection_reports.close()

        if database is not None:
            database.close()
        if database_receive is not None:
            database_receive.close()
        if database_perform is not None:
            database_perform.close()

def run_with_reconnect():
    while not stop_event.is_set():
        try:
            run()
        except pika.exceptions.AMQPError as e:
            logging.error(f"RabbitMQ error, reconectando en 5 segundos: {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error inesperado, reconectando en 5 segundos: {e}")
            time.sleep(5)

if __name__ == '__main__':
    def handle_interrupt(signal_received, frame):
        logging.info("Interrumpido")        
        stop_event.set()
        os._exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)
    run_with_reconnect()