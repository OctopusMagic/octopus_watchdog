import datetime
import logging
import json
import os
import re
import time
import timeit
import threading
import dbf
from dotenv import load_dotenv
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import win32api
from win10toast import ToastNotifier
import msvcrt


logger = logging.getLogger(__name__)
toast = ToastNotifier()
LOCK_FILE = "octopus_watchdog.lock"


class OnMyWatch:
    watchDirectory = os.environ.get("DIRECTORIO_INPUT")

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.watchDirectory, recursive=True)
        logger.info(f"Buscando archivos en el directorio: {self.watchDirectory}")
        show_message("Octopus", f"Buscando archivos en el directorio: {self.watchDirectory}")
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("Servicio detenido.")

        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == "created":
            logger.info(f"Se encontró el archivo: {event.src_path}")
            show_message("Info", "Enviando Archivo a Hacienda, espere...")
            time.sleep(3)
            start = timeit.default_timer()
            send_file_to_api(event.src_path)
            stop = timeit.default_timer()
            logger.info(f"Tiempo de procesamiento: {round(stop - start, 2)} segundos")

            if os.environ.get("BORRAR_ARCHIVOS") == "true":
                os.remove(event.src_path)
                logger.info(f"Archivo eliminado: {event.src_path}")


def extraer_parte_numerica(nombre_archivo):
    patron = r"(\d+)"
    coincidencias = re.search(patron, nombre_archivo)
    if coincidencias:
        return coincidencias.group(1)
    else:
        return ""


def print_file(file_path):
    win32api.ShellExecute(0, "print", file_path, None, ".", 0)


def save_data_to_dbf(data, serie=""):
    archivo_dbf = os.environ.get("ARCHIVO_DBF")
    tabla = dbf.Table(archivo_dbf)
    tabla.open(mode=dbf.READ_WRITE)
    datos = json.loads(data["documento"])
    fechEmi = datos["identificacion"]["fecEmi"]
    fechEmi = datetime.datetime.strptime(fechEmi, "%Y-%m-%d")
    fhProcesamiento = data["fhProcesamiento"]
    fhProcesamiento = datetime.datetime.strptime(fhProcesamiento, "%Y-%m-%dT%H:%M:%SZ")

    if datos["identificacion"]["tipoDte"] == "07":
        montoTotal = datos["resumen"]["totalIVAretenido"]
    elif datos["identificacion"]["tipoDte"] == "14":
        montoTotal = datos["resumen"]["totalCompra"]
    else:
        montoTotal = datos["resumen"]["montoTotalOperacion"]

    tabla.append((
        serie,
        dbf.Date(fechEmi.year, fechEmi.month, fechEmi.day),
        datos["identificacion"]["horEmi"],
        montoTotal,
        data["codGeneracion"],
        datos["identificacion"]["numeroControl"],
        data["selloRecibido"],
        dbf.Date(fhProcesamiento.year, fhProcesamiento.month, fhProcesamiento.day),
        data["estado"],
    ))

    tabla.close()


def anular_dte(file_path):
    file_name = os.path.basename(file_path)
    url = os.environ.get("API_URL")
    endpoint_url = f"{url}/anulacion/txt/"
    with open(file_path, "rb") as file:
        files = {"file": (file_name, file)}
        response = requests.post(endpoint_url, files=files)
        if response.status_code in [500, 400]:
            logger.info(f"DTE Rechazado: {response.text}")
            show_message("Error", f"Error al anular el archivo: {response.text}")
            return
        try:
            data = response.json()
            logger.info(f"DTE Anulado: {data.get('descripcionMsg')}")
        except Exception as e:
            logger.error(f"Error al anular el archivo: {e}")
            show_message("Error", f"Error al anular el archivo: {e}")


def send_file_to_api(file_path):
    file_name = os.path.basename(file_path)
    url = os.environ.get("API_URL")
    if file_name.startswith("CCF"):
        endpoint_url = f"{url}/credito_fiscal/txt/"
    elif file_name.startswith("FAC"):
        endpoint_url = f"{url}/factura/txt/"
    elif file_name.startswith("SUJ"):
        endpoint_url = f"{url}/sujeto_excluido/txt/"
    elif file_name.startswith("CRE"):
        endpoint_url = f"{url}/comprobante_retencion/txt/"
    elif file_name.startswith("NC"):
        endpoint_url = f"{url}/nota_credito/txt/"
    elif file_name.startswith("EXP"):
        endpoint_url = f"{url}/factura_exportacion/txt/"
    elif file_name.startswith("REV"):
        anular_dte(file_path)
        return
    else:
        logger.info(f"Archivo recibido: {os.path.basename(file_path)}")
        return

    with open(file_path, "rb") as file:
        try:
            files = {"file": (file_name, file)}
            response = requests.post(endpoint_url, files=files)
            if response.status_code in [500, 400]:
                logger.error(f"No procesado: {response.text}")
                show_message("Error", f"No se pudo procesar el archivo: {response.text}")
                return

            data = response.json()
            estado = data.get("estado")
            serie = data.get("codigo_serie")
            
            if estado != "RECHAZADO":
                enlace_pdf = data.get("enlace_pdf")
                enlace_json = data.get("enlace_json")
                enlace_rtf = data.get("enlace_rtf")
                folder_name = file_name.split(".")[0]
                output_directory = os.environ.get("DIRECTORIO_OUTPUT")
                folder_path = os.path.join(output_directory, folder_name)
                os.makedirs(folder_path, exist_ok=True)
                pdf_response = requests.get(enlace_pdf)
                pdf_file_path = os.path.join(folder_path, f"{folder_name}.pdf")
                with open(pdf_file_path, "wb") as pdf_file:
                    pdf_file.write(pdf_response.content)
                    logger.info(f"PDF Guardado: {os.path.basename(pdf_file_path)}")

                json_response = requests.get(enlace_json)
                json_file_path = os.path.join(folder_path, f"{folder_name}.json")
                with open(json_file_path, "wb") as json_file:
                    json_file.write(json_response.content)
                    logger.info(f"JSON Guardado: {os.path.basename(json_file_path)}")

                if enlace_rtf != "":
                    rtf_response = requests.get(enlace_rtf)
                    if rtf_response.status_code == 200:
                        rtf_file_path = os.path.join(folder_path, f"{folder_name}_ticket.pdf")
                        with open(rtf_file_path, "wb") as rtf_file:
                            rtf_file.write(rtf_response.content)
                            logger.info(f"Ticket Guardado: {os.path.basename(rtf_file_path)}")
                            if os.environ.get("IMPRIMIR_TICKET") == "true":
                                print_file(rtf_file_path)
                    else:
                        logger.error(f"Error al descargar el Ticket: {rtf_response.text}")
                else:
                    logger.info("No se generó ticket para este DTE")
                
                logger.info(f"Archivo Enviado a Hacienda: {os.path.basename(file_path)}, estado: {estado}")
                show_message("Info", f"{estado}: {os.path.basename(file_path)}")
            else:
                logger.error(f"DTE Rechazado: {data.get('observaciones')}")
                show_message("Error", f"DTE Rechazado: {data.get('observaciones')}")

            save_data_to_dbf(data, serie)

        except Exception as e:
            logger.error(f"Error al enviar el archivo: {e}")
            show_message("Error", f"Error al enviar el archivo: {e}")


def show_message(title, message, type = "error"):
    toast.show_toast(title, message, duration=5, icon_path=None)


def lock_script() -> bool:
    """
    Locks a file pertaining to this script so that it cannot be run simultaneously.
    
    Since the lock is automatically released when this script ends, there is no 
    need for an unlock function for this use case.
    
    Returns:
        True if the lock was acquired, False otherwise.
    
    """
    
    global lockfile  # file must remain open until program quits
    lockfile = open(LOCK_FILE, 'w')

    try:
        # Try to grab an exclusive lock on the file
        msvcrt.locking(lockfile.fileno(), msvcrt.LK_NBLCK, 1)
        
    except IOError:
        return False
        
    else:
        return True


if __name__ == "__main__":
    logging.basicConfig(
        filename='octopus_watchdog.log',
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    load_dotenv()
    
    if not lock_script():
        logger.error("El programa ya está en ejecución.")
        exit(1)

    watch = OnMyWatch()
    # Ejecutar el servicio en un hilo separado
    watch_thread = threading.Thread(target=watch.run)
    watch_thread.daemon = True  # Permite que el hilo se cierre cuando se cierre el programa principal
    watch_thread.start()
    
    # Mantener el programa principal corriendo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Programa detenido.")
        exit(0)
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        exit(1)