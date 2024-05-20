from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime

def send_pending_transactions_email(request):
    # Inicializa el cliente de BigQuery
    client = bigquery.Client()
    
    # Obtiene la fecha de hoy
    today = datetime.date.today()
    
    # Define tu consulta SQL
    query = f"""
        SELECT COUNT(*) as count
        FROM `powerful-star-421901.database.iron`
        WHERE loaded = FALSE
        AND home_env is not null
    """
    
    # Ejecuta la consulta
    query_job = client.query(query)
    results = query_job.result()
    
    # Obtiene el número de registros
    count = 0
    for row in results:
        count = row.count

    # Configuración del correo
    from_email = "team.shield.notification@gmail.com"
    to_email = "nic.davila@duocuc.cl"
    subject = "TRANSACCIONES PENDIENTES BIGQUERY"
    body = f"Cantidad de registros con loaded FALSE para la fecha de hoy ({today}): {count}"

    # Envío del correo
    message = MIMEMultipart()
    message["From"] = from_email
    message["To"] = to_email
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    # Configuración del servidor SMTP
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(from_email, "vrisvphggbqzqffp")
    text = message.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()

    return f"Email sent to {to_email} with {count} pending transactions"

# Asegúrate de reemplazar `your-project` y `your-dataset` con el ID de tu proyecto y el conjunto de datos correcto.
# También, configura `your-email@gmail.com` y `your-email-password` con tu cuenta y contraseña de correo real.
