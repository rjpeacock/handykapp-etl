from datetime import datetime
from typing import Any

from helpers.email_alert import ALERT_SUBJECT_PREFIX, send_email
from helpers.helpers import FetchError


def failure_handler(kind: str, name: str, state: Any) -> None:
    if state.is_failed():
        error_msg = ""
        url = ""
        attempt = 0
        try:
            result = state.result()
            if isinstance(result, FetchError):
                error_msg = str(result)
                url = result.url
                attempt = result.attempt
            else:
                error_msg = str(result)
        except Exception as e:
            error_msg = str(e)

        body_lines = [
            f"{kind}: {name}",
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "Status: FAILED",
            "",
            "Error:",
            error_msg,
        ]
        if url:
            body_lines.insert(4, f"URL: {url}")
            body_lines.insert(5, f"Attempt: {attempt}/5")

        send_email(
            to_address=__import__("os").environ.get(
                "ALERT_EMAIL", "robertjamespeacock@gmail.com"
            ),
            subject=f"{ALERT_SUBJECT_PREFIX} {kind} Failed: {name}",
            body="\n".join(body_lines),
        )
