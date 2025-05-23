import pika
import sys
import os
import json
import time
import signal
from threading import Thread
from .database.database_veolab import DatabaseVeolab
 
def process_received(body, database):
    # Procesa mensajes recibidos en la cola de analíticasRecibidas
    try:
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
    try:
        database = DatabaseVeolab()
        database.open()
        if database.connection is not None:
            reports = database.get_reports()
            if reports is not None:
                for report in reports:
                    queue = report.get('cola') or 'analiticasRealizadas'
                    report_copy = {k: v for k, v in report.items() if k != 'cola'}
                    report_json = json.dumps(report_copy, ensure_ascii=False).encode('utf8')
                    channel.basic_publish(
                        exchange='analiticasRealizadas_exchange', 
                        routing_key=queue, 
                        body=report_json
                    )
                    database.mark_sample_sent(report['codigoEntidadIgeo'])
                    database.logdb("OK", "Informe enviado", report['codigoEntidadIgeo'], True)
        
        print("Procesando informes ...")

    except Exception as e:
        print("Error inesperado:", e)
    finally:
        if database is not None:
            database.close()


def listener_receive(channel, database):
    # Escucha la cola analiticasRecibidas
    def callback(ch, method, properties, body):
        process_received(body, database)

    channel.basic_consume(queue='analiticasRecibidas', on_message_callback=callback, auto_ack=True)
    print("Esperando muestras ...")
    channel.start_consuming()


def listener_perform(channel, database):
    # Escucha la cola resultadoAnaliticasRealizadas
    def callback(ch, method, properties, body):
        process_performed(body, database)

    channel.basic_consume(queue='resultadoAnaliticasRealizadas', on_message_callback=callback, auto_ack=True)
    print("Esperando resultados ...")
    channel.start_consuming()


def run():
    database = None
    database_receive = None
    database_perform = None
    database_reports = None
    connection_receive = None
    connection_perform = None
    try:
        database = DatabaseVeolab()
        database.open()
        rb_config = None
        if database.connection is not None:
            rb_config = database.get_rabbit_config()

        if rb_config is not None:

            credentials = pika.PlainCredentials(rb_config['PARCIGU'], rb_config['PARCIGC'])

            # Inicia el escuchador para la cola analiticasRecibidas 
            database_receive = DatabaseVeolab()
            database_receive.open()
            if database_receive.connection is not None:
                connection_receive = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        rb_config['PARCIGI'],  # Ip
                        rb_config['PARCIGP'],  # Puerto
                        rb_config['PARCIGV'],  # Vhost
                        credentials))
                channel_receive = connection_receive.channel()
                thread_receive = Thread(target=listener_receive, args=(channel_receive, database_receive))
                thread_receive.start()

            # Inicia el escuchador para la cola resultadoAnaliticasRealizadas
            database_perform = DatabaseVeolab()
            database_perform.open()
            if database_perform.connection is not None:
                connection_perform = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        rb_config['PARCIGI'],  # Ip
                        rb_config['PARCIGP'],  # Puerto
                        rb_config['PARCIGV'],  # Vhost
                        credentials))
                channel_perform = connection_perform.channel()            
                thread_perform = Thread(target=listener_perform, args=(channel_perform, database_perform))
                thread_perform.start()

            # Inicia una consulta periódica a la base de datos para procesar informes
            connection_reports = pika.BlockingConnection(
                pika.ConnectionParameters(
                    rb_config['PARCIGI'],  # Ip
                    rb_config['PARCIGP'],  # Puerto
                    rb_config['PARCIGV'],  # Vhost
                    credentials))
            channel_reports = connection_reports.channel()
            seconds = int(0 if rb_config['PARNSEC'] is None else rb_config['PARNSEC']) 
            if seconds <= 0:
                seconds = 60
            def timer():
                while True:
                    process_reports(channel_reports)
                    time.sleep(seconds)  

            thread_report = Thread(target=timer)
            thread_report.start() 

            while thread_receive.is_alive() or thread_perform.is_alive() or thread_report.is_alive():
                time.sleep(1)

    except pika.exceptions.AMQPError as e:
        print("Error de conexión RabbitMQ:", e)
    except Exception as e:
        print("Error inesperado:", e)
    finally:
        if connection_receive is not None:
            connection_receive.close()
        if connection_perform is not None:
            connection_perform.close()
        if connection_reports is not None:
            connection_reports.close()
        if database is not None:
            database.close()
        if database_receive is not None:
            database_receive.close()
        if database_perform is not None:
            database_perform.close()

if __name__ == '__main__':
    def handle_interrupt(signal, frame):
        print ("Interrumpido")        
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)

    run()