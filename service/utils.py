import re
from pathlib import Path
from time import sleep, time
from typing import Any, Iterator, Tuple

from more_itertools import chunked
from prometheus_client import Gauge

from . import config

SLOTS_PER_EPOCH = config['SLOTS_PER_EPOCH']
SECONDS_PER_SLOT = config['SECONDS_PER_SLOT']
ETH1_ADDRESS_LEN = config['ETH1_ADDRESS_LEN']
ETH2_ADDRESS_LEN = config['ETH2_ADDRESS_LEN']

prom_validator_keys = Gauge(
    "monitoring_validator_keys",
    "Количество валидаторов под мониторингом",
)


def hex_to_binary(hex: str) -> list[bool]:
    """Конвертация 16-ого числа в булеан

    Параметры:
    hex: может содержвать `0x`-префикс

    Пример:
    hex_to_binary("0x0F0A") == hex_to_binary("0F0A") == \
    [
        False, False, False, False,
        True, True, True, True,
        False, False, False, False,
        True, False, True, False
    ]
    """
    hex_wo0x = hex[2:] if hex[:2] == "0x" else hex

    hex_wo0x_size = len(hex_wo0x) * 4
    binary = (bin(int(hex_wo0x, 16))[2:]).zfill(hex_wo0x_size)

    return [bit == "1" for bit in binary]


def switch_endianness(bits: list[bool]) -> list[bool]:
    """Переворачивание каждого байта (8 бит)

    Параметры:
    bits: список булеаонов, обозначающих биты

    Пример:
    -------
    switch_endianness(
        [
            False, False, True, False, True, True, True, False,
            True, False, True, False, False, True, True, True
        ]
    ==
        [
            False, True, True, True, False, True, False, False,
            True, True, True, False, False, True, False, True
        ]
    )
    """
    list_of_bits = chunked(bits, 8)
    reversed_list_of_bits = [reversed(bits) for bits in list_of_bits]
    return [item for sublist in reversed_list_of_bits for item in sublist]


def delete_zero_bits(bits: list[bool]) -> list[bool]:
    """Удаление всех элементов из списка после последнего вхождения True

    Параметры:
    bits: список булеанов, означающих биты

    Если все False - возбуждается StopIteration

    Пример:
    --------

    delete_zero_bits([False, True, False, True, True, False]) == \
        [False, True, False, True]
    """
    try:
        index = next((index for index, bit in enumerate(reversed(bits)) if bit))
    except StopIteration:
        raise StopIteration(f"Не найдено ни одного ненулевого бита: {bits}")

    return bits[: -index - 1]


def aggregate_bits(list_of_bools: list[list[bool]]) -> list[bool]:
    """Агрегация булевых значений

    Параметры:
    list_of_bools: Список булеанов

    Примеры:
    ---------
    aggregate_bits(
        [
            [False, False, True],
            [False, True, False]
        ]
    ) == [False, True, True]

    aggregate_bits(
    [
        [False, False],
        [False, True, False]
    ]
    ) ==> ValueError
    """

    _, *trash = {len(bits) for bits in list_of_bools}

    if trash != []:
        raise ValueError("Списки имеют разные длины")

    return [any(bools) for bools in zip(*list_of_bools)]  # тип: игнор


def apply_mask(items: list[Any], mask: list[bool]) -> set[Any]:
    """Применение маски

    Параметры:
    items: Список элементов
    mask: Маска

    Пример:
    --------

    apply_mask(
        ["a", "b", "c", "d", "e"],
        [True, False, False, True, False]
    ) == {"a", "d"}
    """

    return set(item for item, bit in zip(items, mask) if bit)


def load_pubkeys_from_file(path: Path) -> set[str]:
    """Загрузка из файла публичных ключей для мониторинга 

    Параметры:
    path: Путь к файлу, содержащему публичные ключи

    Возвращаемое значение: 
    Множество считанных ключей.
    """
    with path.open() as file_descriptor:
        return set(
            (eth2_address_lower_0x_prefixed(line.strip()) for line in file_descriptor)
        )


def get_own_pubkeys(pubkeys_file_path: Path | None) -> set[str]:
    """Получение публичных ключей валидаторов для мониторинга

    Параметры:
    pubkeys_file_path: Путь к файлу, содержащему публичные ключи

    Если ключи уже есть - возвращаем их.
    """

    # Пока из файла
    pubkeys_from_file: set[str] = (
        load_pubkeys_from_file(pubkeys_file_path)
        if pubkeys_file_path is not None
        else set()
    )

    prom_validator_keys.set(len(pubkeys_from_file))
    return pubkeys_from_file


def get_next_slot(genesis_time_sec: int) -> Iterator[Tuple[int, int]]:
    next_slot = int((time() - genesis_time_sec) / SECONDS_PER_SLOT) + 1

    try:
        while True:
            next_slot_time_sec = genesis_time_sec + next_slot * SECONDS_PER_SLOT
            time_to_wait = next_slot_time_sec - time()
            sleep(max(0, time_to_wait))

            yield next_slot, next_slot_time_sec

            next_slot += 1
    except KeyboardInterrupt:
        pass


def lower_eth1_address(address: str) -> str:
    address_lower = address.lower()

    if not re.match(f"^(0x)?[0-9a-f]{{{ETH1_ADDRESS_LEN}}}$", address_lower):
        raise ValueError(f"Неверный ETH1-адрес: {address_lower}")

    if len(address) == ETH1_ADDRESS_LEN:
        return f"0x{address_lower}"

    return address_lower


def eth2_address_lower_0x_prefixed(address: str) -> str:
    address_lower = address.lower()

    if not re.match(f"^(0x)?[0-9a-f]{{{ETH2_ADDRESS_LEN}}}$", address_lower):
        raise ValueError(f"Неверный ETH2-адрес: {address_lower}")

    if len(address) == ETH2_ADDRESS_LEN:
        return f"0x{address_lower}"

    return address_lower


class LimitedDict:
    def __init__(self, max_size: int) -> None:
        assert max_size >= 0, "Максимальный размер должен быть неотрицательным"

        self.__max_size = max_size
        self.__dict: dict[Any, Any] = dict()

    def __setitem__(self, key: Any, value: Any) -> None:
        self.__dict[key] = value

        first_keys = sorted(self.__dict)[: -self.__max_size]
        for key in first_keys:
            self.__dict.pop(key)

    def __getitem__(self, key: Any) -> Any:
        return self.__dict[key]

    def __contains__(self, key: Any) -> bool:
        return key in self.__dict

    def __len__(self) -> int:
        return len(self.__dict)
