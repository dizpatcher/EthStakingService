"""Заслэшенные и вышедшие валидаторы"""
from prometheus_client import Gauge

from .data_types import Validators
from .notifier import Telegram

prom_own_validators_exited = Gauge(
    "own_validators_exited",
    "Вышедшие из сети валидаторы, находящиеся под мониторингом.",
)

prom_own_validators_slashed = Gauge(
    "own_validators_slashed",
    "Количество наказанных валидаторов под мониторингом",
)

prom_network_validators_slashed = Gauge(
    "network_validators_slahed",
    "Общее количество наказанных валидаторов",
)


class ValidatorsExited:
    """Валидаторы, которые покинули сеть."""

    def __init__(self, telegram: Telegram | None) -> None:
        """Вышедшие валидаторы

        Параметры:
        telegram: Экземпляр телеграм-бота для отправки уведомлений
        """
        self.__own_exited_unslashed_indexes: set[int] | None = None
        self.__telegram = telegram

    def process(
        self,
        own_exited_unslashed_index2validator: dict[int, Validators.DataItem.Validator],
        own_withdrawal_index2validator: dict[int, Validators.DataItem.Validator],
    ) -> None:
        """Обработка вышедших валидаторов.

        Параметры:
        own_exited_unslashed_index2validator: Словарь:
            индекс вышедшего валидатора: данные валидатора
        """

        own_exited_unslashed_indexes = set(own_exited_unslashed_index2validator)

        own_unslashed_withdrawal_index2validator = {
            index
            for index, validator in own_withdrawal_index2validator.items()
            if not validator.slashed
        }

        own_exited_indexes = set(own_exited_unslashed_index2validator) | set(
            own_unslashed_withdrawal_index2validator
        )

        prom_own_validators_exited.set(len(own_exited_indexes))

        if self.__own_exited_unslashed_indexes is None:
            self.__own_exited_unslashed_indexes = own_exited_unslashed_indexes
            return

        own_new_exited_unslashed_indexes = (own_exited_unslashed_indexes - self.__own_exited_unslashed_indexes)

        for index in own_new_exited_unslashed_indexes:
            message = f"🔴 Валидатор {own_exited_unslashed_index2validator[index].pubkey} покинул сеть"
            print(message)

            if self.__telegram is not None:
                self.__telegram.send_broadcast_message(message)

        self.__own_exited_unslashed_indexes = own_exited_unslashed_indexes


class ValidatorsSlashed:

    def __init__(self, telegram: Telegram | None) -> None:
        """

        Параметры:
        telegram: Экземпляр класса телеграм-бота
        """
        self.__total_exited_slashed_indexes: set[int] | None = None
        self.__own_exited_slashed_indexes: set[int] | None = None
        self.__telegram = telegram

    def process(
        self,
        total_exited_slashed_index2validator: dict[
            int, Validators.DataItem.Validator
        ],
        own_exited_slashed_index2validator: dict[int, Validators.DataItem.Validator],
        total_withdrawal_index2validator: dict[int, Validators.DataItem.Validator],
        own_withdrawal_index2validator: dict[int, Validators.DataItem.Validator],
    ) -> None:
        """Обработка заслешенных валидаторов

        Параметры:
        total_exited_slashed_index2validator: Словарь:
            индекс заслешенного валидатора в сети[данные валидатора]
        own_exited_slashed_index2validator  : Словарь:
            индекс заслешенного валидатора под мониторингом[данные валидатора]
        total_withdrawal_index2validator    : Словарь:
            индекс вышедшего валидатора в сети[данные валидатора]
        own_withdrawal_index2validator      : Словарь:
            индекс вышедшего валидатора под мониторингом[данные валидатора]
        """
        total_slashed_withdrawal_index2validator = {
            index
            for index, validator in total_withdrawal_index2validator.items()
            if validator.slashed
        }

        own_slashed_withdrawal_index2validator = {
            index
            for index, validator in own_withdrawal_index2validator.items()
            if validator.slashed
        }

        total_slashed_indexes = set(total_exited_slashed_index2validator) | set(
            total_slashed_withdrawal_index2validator
        )

        own_slashed_indexes = set(own_exited_slashed_index2validator) | set(
            own_slashed_withdrawal_index2validator
        )

        prom_network_validators_slashed.set(len(total_slashed_indexes))
        prom_own_validators_slashed.set(len(own_slashed_indexes))

        total_exited_slashed_indexes = set(total_exited_slashed_index2validator)
        own_exited_slashed_indexes = set(own_exited_slashed_index2validator)

        if (
            self.__total_exited_slashed_indexes is None
            or self.__own_exited_slashed_indexes is None
        ):
            self.__total_exited_slashed_indexes = total_exited_slashed_indexes
            self.__own_exited_slashed_indexes = own_exited_slashed_indexes
            return

        total_new_exited_slashed_indexes = (
            total_exited_slashed_indexes - self.__total_exited_slashed_indexes
        )

        own_new_exited_slashed_indexes = (
            own_exited_slashed_indexes - self.__own_exited_slashed_indexes
        )

        not_own_new_exited_slashed_indexes = (
            total_new_exited_slashed_indexes - own_new_exited_slashed_indexes
        )

        for index in not_own_new_exited_slashed_indexes:
            message = f"📛 Валидатор {not_own_new_exited_slashed_indexes[index].pubkey} заслешен"
            print(message)

        for index in own_new_exited_slashed_indexes:
            message = f"📛 Валидатор под мониторингом {own_exited_slashed_index2validator[index].pubkey} заслешен"
            print(message)

            if self.__telegram is not None:
                self.__telegram.send_broadcast_message(message)

        self.__total_exited_slashed_indexes = total_exited_slashed_indexes
        self.__own_exited_slashed_indexes = own_exited_slashed_indexes
