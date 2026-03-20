from datetime import datetime
from typing import Any

from helpers.email_alert import ALERT_SUBJECT_PREFIX, send_email


def failure_handler(kind: str, name: str, state: Any) -> None:
    if state.is_failed():
        error_msg = ""
        try:
            error_msg = state.result()
        except Exception as e:
            error_msg = str(e)

        body = f"""
{kind}: {name}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Status: FAILED

Error:
{error_msg}
"""
        send_email(
            to_address=__import__("os").environ.get(
                "ALERT_EMAIL", "robertjamespeacock@gmail.com"
            ),
            subject=f"{ALERT_SUBJECT_PREFIX} {kind} Failed: {name}",
            body=body,
        )
