import os
import smtplib
from email.mime.text import MIMEText

ALERT_SUBJECT_PREFIX = "[handykapp-etl]"
GMAIL_SMTP = "smtp.gmail.com"
GMAIL_PORT = 587


def send_email(
    to_address: str,
    subject: str,
    body: str,
    from_address: str | None = None,
    password: str | None = None,
) -> None:
    from_address = from_address or os.environ.get("GMAIL_ADDRESS")
    password = password or os.environ.get("GMAIL_APP_PASSWORD")

    if not from_address or not password:
        raise ValueError("Gmail credentials not configured")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = to_address

    with smtplib.SMTP(GMAIL_SMTP, GMAIL_PORT) as server:
        server.starttls()
        server.login(from_address, password)
        server.send_message(msg)
