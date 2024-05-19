"""Логика вычисления продолжительности очереди активации валидаторов."""

from prometheus_client import Gauge
from . import config

MIN_PER_EPOCH_CHURN_LIMIT = config['MIN_PER_EPOCH_CHURN_LIMIT']
CHURN_LIMIT_QUOTIENT = config['CHURN_LIMIT_QUOTIENT']
MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT = config['MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT'] # Dencun hardfork https://github.com/ethereum/consensus-specs/blob/46b118a212a27e5e3ea9b9b079e17952d94b9d45/configs/mainnet.yaml#L89
SECONDS_PER_SLOT = config['SECONDS_PER_SLOT']
SLOTS_PER_EPOCH = config['SLOTS_PER_EPOCH']
SECONDS_PER_EPOCH = config['SECONDS_PER_SLOT'] * config['SLOTS_PER_EPOCH']

prom_activation_queue = Gauge(
    "activation_queue_gauge",
    "Очередь на активацию в секундах",
)


def get_activation_churn(n_active_validators: int) -> int:
    """Вычисление количества валидаторов, которые могут стать активными за одну эпоху

    Параметры:
    n_active_validators: Количество активных на текущий момент валидаторов
    """

    return min(MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT, n_active_validators // CHURN_LIMIT_QUOTIENT)


def compute_activation_duration(
    n_active_validators: int, n_pending_validators: int
) -> int:
    """Вычисление оставшегося времени до активации валидатора, если ни один из валидаторов не собирается выходить.

    Параметры:
    n_active_validators   : Количество активных на текущий момент валидаторов
    n_pending_validators: Позиция валидатора в очереди на вход
    """

    activation_churn = get_activation_churn(n_active_validators)
    epochs2proceed_queue = int(n_pending_validators / activation_churn)

    return epochs2proceed_queue * SECONDS_PER_EPOCH


def export_activation_duration(
    n_active_validators: int, n_pending_validators: int
) -> None:
    """Экспорт времени в Prometheus

    Функция берёт среднее между пессимистичным и оптимистичным временем

    Параметры:
    n_active_validators    : Количество активных на текущий момент валидаторов
    n_pending_validators   : Позиция валидатора в очереди на вход
    """

    duration_sec = compute_activation_duration(n_active_validators, n_pending_validators)
    prom_activation_queue.set(duration_sec)
