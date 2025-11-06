import smtplib
import traceback
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def send_error_email(e, context):
    """Sends an error email notification."""
    sender = os.getenv('ERROR_EMAIL_SENDER')
    receivers = os.getenv('ERROR_EMAIL_RECEIVERS')
    smtp_server = os.getenv('ERROR_EMAIL_SMTP_SERVER')

    if not sender or not receivers or not smtp_server:
        logger.warning("Email sending is not configured properly. Skipping error email.")
        return
    
    receivers = [email.strip() for email in receivers.split(',')]

    message = f"""From: PyGeoAPI Error Monitor <{sender}>
        To: Admin <{receivers[0]}>
        Subject: [Laji-PyGeoAPI Error] {context}

        Error occurred in when loading data to the PyGeoAPI:

        Context: {context}
        Error Type: {type(e).__name__}
        Error Message: {str(e)}
        Timestamp: {datetime.now().isoformat()}

        Traceback:
        {traceback.format_exc()}
    """

    try:
        smtpObj = smtplib.SMTP(smtp_server)
        smtpObj.sendmail(sender, receivers, message)
        logger.info("Successfully sent error notification email")
    except smtplib.SMTPException:
        logger.error("Error: unable to send email")

def test_send_error_email():
    # Example usage
    try:
        # Simulate some code that raises an exception
        1 / 0
    except Exception as e:
        send_error_email(e, "division by zero in example usage")