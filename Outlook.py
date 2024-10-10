import win32com.client as win32
import logging
import Config


def notificacion_outlook(email:list,cc:list,message:str,asunto:str,rutas:list=None):
    try:
        logging.info(f'Iniciando el envío de correo a {email} con asunto "{asunto}"')
        # Crear una instancia de Outlook
        outlook = win32.Dispatch('Outlook.Application')
        namespace = outlook.GetNamespace("MAPI")

        # Crear un objeto de correo electrónico
        mail = outlook.CreateItem(0)

        # Puedes proporcionar una lista de direcciones de correo electrónico separadas por punto y coma (;)
        mail.To = ";".join(email)
        mail.cc = ";".join(cc)
        # Configurar el correo electrónico
        mail.Subject = asunto
        
        message_html = message
        mail.BodyFormat = 2  # 2 significa HTML
        mail.HTMLBody = message_html

        if rutas:
            for ruta in rutas:
                mail.Attachments.Add(ruta)
                logging.info(f'Archivo adjuntado: {ruta}')
        else:
            logging.info('No se adjuntaron archivos.')


        mail.Send()
    except Exception as e:
        logging.error(f'Error al enviar el correo a {email}: {str(e)}')
        raise

def manejar_error(dia):
    try:
        with open(Config.Mensaje_html, encoding='utf-8') as Mensaje:
            mensaje_html = Mensaje.read()

        fecha = dia.strftime('%Y-%m-%d')  # Formatear la fecha
        formato = "F_523"
        mensaje_error = f"No se obtiene respuesta de la {Config.url_523}"

        mensaje_html = mensaje_html.replace("_fecha_", fecha)
        mensaje_html = mensaje_html.replace("_Formato_", formato)
        mensaje_html = mensaje_html.replace("_Mensaje_error_", mensaje_error)

        asunto = f"{dia.strftime('%Y-%m-%d')} Error Conexion Datos Abiertos"
        destinatarios = Config.config_odbc(Config.destinatarios)

        notificacion_outlook(
            destinatarios['destinatarios']['mensaje_Error'],
            destinatarios['Con_Copia']['mensaje_Error'],
            message=mensaje_html,
            asunto=asunto
        )
        SystemExit

    except FileNotFoundError:
        logging.error("El archivo Mensaje_Falla.txt no fue encontrado.")
    except Exception as e:
        logging.error(f"Ocurrió un error al leer el archivo: {str(e)}")
