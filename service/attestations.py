import math
import functools
from collections import defaultdict

from prometheus_client import Gauge

from .cl_node import ConsensusNode
from .data_types import Block, Validators, ConsensusClient
from .notifier import Telegram
from .utils import (
    LimitedDict,
    aggregate_bits,
    apply_mask,
    hex_to_binary,
    delete_zero_bits,
    switch_endianness,
)

from . import config

SLOTS_PER_EPOCH = config['SLOTS_PER_EPOCH']

print = functools.partial(print, flush=True)

prom_missed_attestations = Gauge("n_missed_attestations", "Количество пропущенных аттестаций")
prom_paired_missed_attestations = Gauge("n_paired_missed_attestations", "Количество пропущенных аттестаций подряд")
prom_participation_rate = Gauge("attestation_rate_gauge", "Уровень участия в консенсусе")

def process_missed_attestations(
    cl_node: ConsensusNode,
    cl_client: ConsensusClient,
    epoch2index2validator_index: LimitedDict,
    epoch: int,
    telegram: Telegram | None,
) -> set[int]:
    """Обработка пропущенных аттестаций.

    Параметры:
    cl_node                      : Beacon-нода
    cl_client                    : Консенсус клиент (LH, Prysm и тд)
    epoch2index2validator_index  : Словарь:
        эпоха[индекс валидатора[данные валидатора]]
    epoch                        : Эпоха, для которой пропускаются аттестации
    """
    if epoch < 1:
        return set()

    index2validator: dict[int, Validators.DataItem.Validator] = (
        epoch2index2validator_index[epoch - 1]
        if epoch - 1 in epoch2index2validator_index
        else epoch2index2validator_index[epoch]
    )

    validators_index = set(index2validator)
    validators_liveness = cl_node.get_validators_liveness(
        cl_client, epoch - 1, validators_index
    )

    dead_indexes = {
        index for index, liveness in validators_liveness.items() if not liveness
    }

    prom_missed_attestations.set(len(dead_indexes))

    if len(dead_indexes) == 0:
        return set()

    first_indexes = list([str(idx) for idx in dead_indexes])[:5]
    
    n_extra_vals = len(dead_indexes) - len(first_indexes)
    message = f"📢 Валидаторы {', '.join(first_indexes)}{f' и ещё {n_extra_vals} ' if n_extra_vals > 0 else ''} не предоставили аттестации в эпохе {epoch - 1}"

    if telegram is not None:
        telegram.send_broadcast_message(message)

    print(message)

    return dead_indexes


def process_paired_missed_attestations(
    dead_indexes: set[int],
    previous_dead_indexes: set[int],
    epoch: int,
    telegram: Telegram | None,
) -> set[int]:
    """Пропуск двух аттестаций подряд

    Подряд:
    dead_indexes                 : Индексы валидаторов, которые пропустили аттестации
    previous_dead_indexes        : Множество индексов валидаторов, которые пропустили аттестации в предыдущей эпохе

    epoch                        : Эпоха, для которой проверяются пропущенные аттестации
    telegram                     : Экземпляр класса телеграм-бота
    """
    if epoch < 2:
        return set()

    double_dead_indexes = dead_indexes & previous_dead_indexes
    prom_paired_missed_attestations.set(len(double_dead_indexes))

    if len(double_dead_indexes) == 0:
        return set()

    first_indexes = list([str(idx) for idx in double_dead_indexes])[:5]

    n_extra_vals = len(double_dead_indexes) - len(first_indexes)
    message = f"""
        🔊 Валидаторы {', '.join(first_indexes)}{f' и ещё {n_extra_vals} ' if n_extra_vals > 0 else ''} пропустили 2 аттестации подряд с эпохи {epoch - 2}
        Возможно проблемы с нодой!
    """

    print(message)

    if telegram is not None:
        telegram.send_broadcast_message(message)

    return double_dead_indexes


def process_attestations(
    cl_node: ConsensusNode,
    block: Block,
    slot: int,
    own_active_validators_index2validator: dict[int, Validators.DataItem.Validator],
    telegram: Telegram | None = None
) -> set[int]:
    """Обработка аттестаций

    Параметры:
    cl_node                              : Beacon-нода
    block                                : Блок, для которого ищутся неоптимальные аттестации
    slot                                 : Слот
    own_active_validators_index2validator: Словарь:
      индекс валидатора[публичный ключ валидатора]
    telegram: Объект мессенджера для рассылки сообщений
    """
    if slot < 1:
        return set()

    previous_slot = slot - 1

    # Эпоха предыдущего слота - это не предыдущая эпоха, а действительно эпоха, соответствующая предыдущему слоту.
    epoch_of_previous_slot = math.floor(previous_slot / SLOTS_PER_EPOCH)

    # Все индексы валидаторов под мониторингом
    own_active_validators_index = set(own_active_validators_index2validator)

    # Вложенный словарь
    # слот[индекс коммитета[список валидаторов, которые должны аттеставать слот в данном коммитете]]
    duty_slot2committee_index2validators_index: dict[
        int, dict[int, list[int]]
    ] = cl_node.get_duty_slot2committee_index2validators_index(
        epoch_of_previous_slot
    )

    # Словарь
    # индекс коммитета [список валидаторов в коммитете, которые должны были провести аттестацию в предыдущем слоте]
    duty_committee_index2validators_index_during_previous_slot = (
        duty_slot2committee_index2validators_index[previous_slot]
    )

    # Индексы валидаторов, которые должны были провести аттестацию в предыдущем слоте
    own_validators_index_attested_during_previous_slot = set(
        (
            item
            for sublist in duty_committee_index2validators_index_during_previous_slot.values()
            for item in sublist
        )
    )

    # Индексы валидаторов под мониторингом, которые должны были провести аттестацию в предыдущем слоте
    own_validators_index_attested_during_previous_slot = (
        own_validators_index_attested_during_previous_slot
        & own_active_validators_index
    )

    # Словарь
    # Индекс комиитета [список булевых значений, означающих успешную или нет аттестацию]
    committee_index2validator_attestation_success = aggregate_attestations(
        block, previous_slot
    )

    list_of_validators_index_that_attested_optimally_during_previous_slot = (
        apply_mask(
            duty_committee_index2validators_index_during_previous_slot[
                actual_committee_index
            ],
            validator_attestation_success,
        )
        for (
            actual_committee_index,
            validator_attestation_success,
        ) in committee_index2validator_attestation_success.items()
    )

    # Валидаторы, которые предоставили аттестации в предыдущем слоте
    validators_index_that_attested_optimally_during_previous_slot: set[int] = set(
        item
        for sublist in list_of_validators_index_that_attested_optimally_during_previous_slot
        for item in sublist
    )

    # Индексы валидаторов под мониторингом, которое провели оптимальную аттестацию в предыдущем слоте
    own_validators_index_that_attested_optimally_during_previous_slot = (
        validators_index_that_attested_optimally_during_previous_slot
        & own_validators_index_attested_during_previous_slot
    )

    # Индексы валидаторов под мониторингом, которые не провели оптимальную аттестацию в предыдущем слоте
    own_validators_wo_optimal_attesting_prev_slot = (
        own_validators_index_attested_during_previous_slot
        - own_validators_index_that_attested_optimally_during_previous_slot
    )

    attestation_rate = (
        len(own_validators_index_that_attested_optimally_during_previous_slot)
        / len(own_validators_index_attested_during_previous_slot)
        if len(own_validators_index_attested_during_previous_slot) != 0
        else None
    )

    if attestation_rate is not None:
        prom_participation_rate.set(100 * attestation_rate)

    if len(own_validators_wo_optimal_attesting_prev_slot) > 0:
        assert attestation_rate is not None

        first_indexes = sorted(list([str(idx) for idx in own_validators_wo_optimal_attesting_prev_slot]))[:5]

        n_extra_vals = len(own_validators_wo_optimal_attesting_prev_slot) - len(first_indexes)
        message = f"""
            ❗ Валидаторы под мониторингом {', '.join(first_indexes)}{f' и ещё {n_extra_vals} ' if n_extra_vals > 0 else ''} не предоставили аттестации для слота {previous_slot}
            (participation rate = {round(100 * attestation_rate, 1)}%)
            """
        
        if telegram is not None:
            telegram.send_broadcast_message(message)
        print(message)

    return own_validators_wo_optimal_attesting_prev_slot


def aggregate_attestations(block: Block, slot: int) -> dict[int, list[bool]]:
    """Объединение всех аттестаций.

    Параметры:
    block: Блок
    slot: Слот

    Возвращаемое значение:
    Каждое булевое значение в списке означает проведение аттестации конкретным валидаторов в коммитете
    Если аттестация валидатора из предыдущего слота включена в текущий, то значени = True
    """
    filtered_attestations = (attestation for attestation in block.data.message.body.attestations 
                             if attestation.data.slot == slot)

    # TODO: dict comprehension
    committee_index2aggregation_bits: dict[
        int, list[list[bool]]
    ] = defaultdict(list)

    for attestation in filtered_attestations:
        aggregated_bits_little_endian_with_last_bit = attestation.aggregation_bits

        # Агрегация битов из 16-ого формата - конвертация байтов в булеан
        aggregated_bits_little_endian_with_last_bit = hex_to_binary(aggregated_bits_little_endian_with_last_bit)

        # Биты агрегации в little endian.
        # Однако, валидаторы в коммитете расставлены в big endian формате.
        # Приводим всё к big endian
        aggregated_bits_with_last_bit = switch_endianness(aggregated_bits_little_endian_with_last_bit)

        # Один бит на одного валидатора (в aggregations bits)
        # Бит числа валидаторов всегда кратен 8, даже если число валидаторов не кратно 8. 
        # Последний "1" (или последнее True) представляет границу. 
        # Все следующие "0" могут быть проигнорированы, поскольку они не представляют валидаторы
        # поэтому последняя "1" и все последующие "0" удаляютcя
        aggregated_bits = delete_zero_bits(aggregated_bits_with_last_bit)

        committee_index2aggregation_bits[attestation.data.index].append(aggregated_bits)

    # Агрегация аттестаций
    items = committee_index2aggregation_bits.items()

    return {
        committee_index: aggregate_bits(aggregation_bits)
        for committee_index, aggregation_bits in items
    }
