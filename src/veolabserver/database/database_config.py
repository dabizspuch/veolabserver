import os
import sys
import configparser
import base64
from getpass import getpass
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from dotenv import load_dotenv

class DatabaseConfig(object):
    """
    Esta clase accede o solicita las credenciales de conexión a la base de datos.
    El archivo config.ini registra la información.    
    """

    load_dotenv()

    KEY_ENV = os.getenv('VEOLAB_AES_KEY')
    if not KEY_ENV:
        raise ValueError("Establezca el valor de la clave VEOLAB_AES_KEY en .env")
    KEY = base64.b64decode(KEY_ENV)
    
    FILENAME = 'config.ini'  # Nombre del archivo de configuración

    def __init__(self, host=None, port=None, database=None, user=None, passwd=None):        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.passwd = passwd
        self.config_path = os.path.join(os.path.dirname(__file__), self.FILENAME)

    def input_config(self):
        print("A continuación se solicitarán los datos de conexión.")
        
        try:
            self.host = input("Servidor: ")
            self.port = input("Puerto: ")
            self.database = input("Base de datos: ")
            self.user = input ("Usuario: ")
            self.passwd = getpass ("Contraseña: ")
        except KeyboardInterrupt:
            sys.exit()    

        self.write_config()

    def read_config(self):
        config = configparser.ConfigParser()
        config.read(self.config_path)

        if not config.sections():
            print("No se ha configurado la conexión a la base de datos.")
            print(self.config_path)
            self.input_config()
        else:
            try:
                self.host = config['conection']['host']
                self.port = config['conection']['port']
                self.database = config['conection']['database']
                self.user = config['conection']['user']
                passwd_encrypt = bytes.fromhex(config['conection']['passwd'])
                iv = passwd_encrypt[:AES.block_size]
                passwd_encrypt = passwd_encrypt[AES.block_size:]
                cipher = AES.new(self.KEY, AES.MODE_CFB, iv)
                self.passwd = cipher.decrypt(passwd_encrypt).decode()
            except (KeyError, ValueError) as e:
                print("El archivo de configuración es inválido:", e)
                self.input_config()

    def write_config(self):
        config = configparser.ConfigParser()
        
        iv = get_random_bytes(AES.block_size)
        cipher = AES.new(self.KEY, AES.MODE_CFB, iv)
        passwd_encrypt = iv + cipher.encrypt(self.passwd.encode())

        config['conection'] = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'passwd': passwd_encrypt.hex()
        }
        try:
            with open(self.config_path, 'w') as f:
                config.write(f)
        except OSError as e:
            print("Error al escribir el archivo de configuración:", e)