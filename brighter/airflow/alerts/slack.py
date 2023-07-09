import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable, Optional

import pytz
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.email import send_email

SLACK_CONN_ID = "slack_connection"


@dataclass
class Alert:
    subject: str
    html_content: str
    files: Optional[list[str]] = None


class AlertAbstractFactory:
    registry: dict[str, Callable[[Any], Alert]] = {}

    @classmethod
    def register(cls, alert_id: str) -> Callable:
        def inner_wrapper(factory: Callable[[Any], Alert]) -> Callable:
            cls.registry[alert_id] = factory
            return factory

        return inner_wrapper

    @classmethod
    def create(cls, alert_id: str) -> Callable[[Any], Alert]:
        factory = cls.registry.get(alert_id, None)
        assert factory is not None
        return factory


def convert_datetime(date: dt.datetime) -> str:
    """Convierte una fecha a zona horaria de Buenos Aires.

    Args:
        date (dt.datetime): Fecha a convertir.

    Returns:
        str: Fecha en zona horaria de Buenos Aires en string.
    """
    return date.astimezone(
        pytz.timezone("America/Argentina/Buenos_Aires")
    ).strftime("%b-%d %H:%M:%S")


def slack_fail_alert(context: Any) -> None:
    """Envia una alerta a slack.

    Args:
        context (Any): Contexto de airflow.
    """
    connection = BaseHook.get_connection(SLACK_CONN_ID)
    channel = connection.login
    slack_webhook_token = connection.password
    slack_msg = (
        "<!channel>\n"
        f"*DAG*: {context.get('task_instance').dag_id}\n"
        f"*Task*: {context.get('task_instance').task_id}\n"
        f"*Fecha*: {convert_datetime(context.get('logical_date'))}\n"
        f"*{type(context.get('exception')).__name__}*: {context.get('exception')}\n"
        f"*Logs*: <{context.get('task_instance').log_url}|*url*>\n"
    )

    slack_alert = SlackWebhookOperator(
        task_id="slack_fail",
        channel=channel,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
        http_conn_id=SLACK_CONN_ID,
    )

    return slack_alert.execute(context=context)


def email_alert(alert_id: str, **kwargs) -> None:
    """Envia un email con la alerta indicada.

    Args:
        alert_id (str): Alerta a enviar.
    """
    from airflow.models import Variable

    email_address = Variable.get("email_notificaciones")
    alert_factory = AlertAbstractFactory.create(alert_id)
    alert = alert_factory(**kwargs)
    send_email(
        to=email_address,
        subject=alert.subject,
        html_content=alert.html_content,
        files=alert.files,
    )
