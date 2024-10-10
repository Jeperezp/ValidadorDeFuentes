# **Automatización de Validación de Datos**

---

## **Descripción General:**

Este repositorio contiene un script en Python diseñado para automatizar la validación de datos utilizando varios recursos externos, como días festivos, consultas a bases de datos SQL y notificaciones por correo electrónico mediante Outlook. El proceso gestiona carpetas, consulta bases de datos y envía reportes automáticos sobre el estado de la validación.

---

## **Características Clave:**

- **Gestión de Carpetas:** Creación dinámica de carpetas y eliminación de archivos antiguos en directorios específicos.
- **Procesamiento de Festivos:** Verificación de días festivos en Colombia, asegurando la ejecución en días laborales.
- **Consultas SQL:** Ejecución de consultas SQL sobre un periodo de tiempo determinado y comparación de dos fuentes de datos (una Base de Datos y "Datos Abiertos").
- **Registro de Eventos (Logging):** Registro detallado de acciones y posibles errores a lo largo del proceso.
- **Notificación Automática por Correo:** Generación y envío de reportes por correo electrónico con los resultados de la validación.

---

## **¿Cómo Funciona?**

1. **Configuración de Fechas y Logging:**
   - Se establece la fecha actual y se configura el sistema de logging para registrar la ejecución y los errores.

2. **Gestión de Carpetas:**
   - Se crean carpetas para almacenar los resultados y se eliminan archivos antiguos de procesos previos.

3. **Verificación de Festivos:**
   - Utiliza datos de días festivos para asegurar que el proceso se ejecute solo en días laborales.

4. **Consultas SQL:**
   - Ejecuta una consulta SQL para un rango de fechas y compara los resultados de dos fuentes de datos.

5. **Validación de Datos:**
   - Valida si los datos están completos y si hay discrepancias entre las dos fuentes de datos.

6. **Notificación por Correo:**
   - Envía un correo en formato HTML con el resultado del proceso, adjuntando cualquier reporte generado.

---

## Descripción de los Módulos

- **Config.py**: Carga de las variables globales y rutas de archivos desde el archivo de configuración (`.env`).
- **Folders.py**: Contiene funciones para la creación de carpetas, eliminación de archivos de una ruta, y consolidación de archivos en un solo directorio.
- **Types.py**: Maneja la transformación de los tipos de datos de las columnas de un DataFrame.
- **Utils.py**: Incluye las funciones para conectar con la base de datos, hacer consultas a APIs, y realizar validaciones de fechas como días hábiles, feriados, y el manejo de fechas por lotes.
- **Outlook.py**: Módulo que permite el envío de correos electrónicos con notificaciones, ya sea en caso de errores o al finalizar el proceso con éxito.

## Configuración

El proyecto utiliza un archivo `.env` para gestionar las rutas y las configuraciones de las fuentes de datos. 


## **Requisitos:**

- Python 3.12.2
- Librerías necesarias (pueden instalarse con `pip install -r requirements.txt`):
  - `pandas`
  - `datetime`
  - `holidays_co`
  - `os`
  - `logging`
  - `requests`
  - `numpy`
  - `sys`

---

## **Instrucciones para Ejecutar:**

1. Clona este repositorio en tu entorno local:
   ```bash
   git clone <URL_DEL_REPOSITORIO>