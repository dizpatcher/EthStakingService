"""Обнаружение пропущенных блоков и получение информации о том, что валидатор назначен для выпуска блока"""

import math
import functools
import requests

from prometheus_client import Counter, Gauge

from .cl_node import ConsensusNode, NoBlockError
from .data_types import Block, BlockTerm
from .notifier import Telegram
from . import config

print = functools.partial(print, flush=True)

SLOTS_PER_EPOCH = config['SLOTS_PER_EPOCH']
MEV_RELAYS = config['MEV_RELAYS']
RELAY_PAYLOAD_URL = config['RELAY_PAYLOAD_URL']

prom_missed_head_proposal = Counter(
    "n_missed_head_proposals",
    "Количество пропущенных блоков",
)

prom_missed_finalized_proposal = Counter(
    "n_missed_finalized_proposals",
    "Количество зафинализированных пропущенных блоков",
)

prom_future_proposal = Gauge(
    "future_proposals",
    "Количество ожидаемых блоков для выпуска",
)

prom_blocks_processed = Counter(
    'n_blocks',
    'Количество обработанных блоков',
)

prom_mevblocks_processed = Counter(
    'n_mevblocks',
    'Количество MEV-блоков',
)

prom_mevblocks_rewards_count = Counter(
    'mevblock_rewards',
    'Сумма вознаграждений за производство блоков',
)

def process_block_proposal(slot: int, block: Block = None) -> None:
    """Извлечение информации для выводов об уровне Maximal Extracted Value в сети"""

    if not slot: 
        slot = block.data.message.slot
    prom_blocks_processed.inc()
    
    for relay in MEV_RELAYS.values():
        url = f'{relay}{RELAY_PAYLOAD_URL}?slot={slot}'
        try:
            r = requests.get(url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(f"HTTP ошибка: {err}")
            continue
        except requests.exceptions.SSLError as err:
            print(f"SSLError ошибка: {err}")
            continue
        
        mevblock_data = r.json()
        if mevblock_data:
            prom_mevblocks_processed.inc()
            mev_reward = int(mevblock_data[0].get('value', 0)) / 10**18
            return mev_reward
        return 0


def process_missed_blocks_head(
    cl_node: ConsensusNode,
    block: Block | None,
    slot: int,
    own_pubkeys: set[str],
    telegram: Telegram | None
) -> bool:
    """Обработка пропущенных блоков без финализации

    Параметры:
    cl_node        : Beacon-нода
    potential_block: Блок
    slot           : Слот
    own_pubkeys    : Множество ключей валидатора
    telegram       : Экземляр класса телеграм-бота

    True если блок был выпущен
    """
    missed = block is None
    epoch = math.floor(slot / SLOTS_PER_EPOCH)
    proposer_duties = cl_node.get_proposer_duties(epoch)

    # Получить публичный ключ валидатора, который должен создать блок
    proposer_duties_data = proposer_duties.data

    # возвращаемое значение - список
    proposer_pubkey = next(
        (
            proposer_duty_data.pubkey
            for proposer_duty_data in proposer_duties_data
            if proposer_duty_data.slot == slot
        )
    )

    # Проверка, что валидатор находится под мониторингом
    is_own_validator = proposer_pubkey in own_pubkeys
    positive_sign = '✨' if is_own_validator else '☑️'
    negative_sign = '🔺' if is_own_validator else '⚠️'

    emoji, proposed_or_missed = (
        (negative_sign, "пропустил  ") if missed else (positive_sign, "выпустил")
    )

    message = f"""
        {emoji} Валидатор {'под мониторингом ' if is_own_validator else ''}{proposer_pubkey}
        {proposed_or_missed} блок в эпохе {epoch} - слот {slot} {emoji} 
        🔑 {len(own_pubkeys)} ключей под мониторингом
    """

    print(message)
    if telegram is not None and missed and is_own_validator:
        telegram.send_broadcast_message(message)

    if is_own_validator and missed:
        prom_missed_head_proposal.inc()

    return is_own_validator


def process_missed_blocks_finalized(
    cl_node: ConsensusNode,
    last_processed_finalized_slot: int,
    slot: int,
    block_reward: int,
    own_pubkeys: set[str],
    telegram: Telegram | None,
) -> int:
    """Обработка пропущенных блоков без финализации

    Параметры:
    cl_node        : Beacon-нода
    potential_block: Номер блока
    slot           : Слот
    own_pubkeys    : Множество ключей валидаторов для мониторинга
    telegram       : Экземляр класса тг-бота

    Возвращает последний финализированный блок
    """
    assert last_processed_finalized_slot <= slot, "Последний финализированный слот > слот"

    last_finalized_header = cl_node.get_header(BlockTerm.FINALIZED)
    last_finalized_slot = last_finalized_header.data.header.message.slot
    epoch_of_last_finalized_slot = math.floor(last_finalized_slot / SLOTS_PER_EPOCH)

    # Возможно отсутствия данных для очень старой эпохи
    cl_node.get_proposer_duties(epoch_of_last_finalized_slot)

    for slot_ in range(last_processed_finalized_slot + 1, last_finalized_slot + 1):
        epoch = math.floor(slot_ / SLOTS_PER_EPOCH)
        proposer_duties = cl_node.get_proposer_duties(epoch)

        # Получение валидатора, назначенного для выпуска блока
        proposer_duties_data = proposer_duties.data

        # данные - список
        proposer_pubkey = next(
            (
                proposer_duty_data.pubkey
                for proposer_duty_data in proposer_duties_data
                if proposer_duty_data.slot == slot_
            )
        )

        # Проверка на нахождение валидатора в списке для мониторинга
        is_own_validator = proposer_pubkey in own_pubkeys

        if not is_own_validator:
            continue

        # Проверка, что блок был выпущен
        try:
            cl_node.get_header(slot_)
            message = f"""
                    📈 Валидатор `{proposer_pubkey}` создал блок! (слот {slot_}) 
                    Вознаграждение за него: {block_reward} ETH 📈
                    """
            prom_mevblocks_rewards_count.inc(block_reward)

        except NoBlockError:

            message = f"❌ Валидатор под мониторингом `{proposer_pubkey}` пропустил блок в эпохе {epoch} - слот {slot_} ❌"
            prom_missed_finalized_proposal.inc()

        print(message)

        if telegram is not None:
            telegram.send_broadcast_message(message)

    return last_finalized_slot


def process_future_blocks_proposal(
    cl_node: ConsensusNode,
    own_pubkeys: set[str],
    slot: int,
    is_new_epoch: bool,
    telegram: Telegram | None = None
) -> int:
    """Обработка следующих выпусков блоков

    Параметры:
    beacon      : Beacon-нода
    own_pubkeys : Множество публичных ключей валидатора
    slot        : Слот
    is_new_epoch: Новая ли эпоха
    """
    epoch = math.floor(slot / SLOTS_PER_EPOCH)
    proposers_duties_current_epoch = cl_node.get_proposer_duties(epoch)
    proposers_duties_next_epoch = cl_node.get_proposer_duties(epoch + 1)

    concatenated_data = (
        proposers_duties_current_epoch.data + proposers_duties_next_epoch.data
    )

    filtered = [
        item
        for item in concatenated_data
        if item.pubkey in own_pubkeys and item.slot >= slot
    ]

    prom_future_proposal.set(len(filtered))

    if is_new_epoch:
        for item in filtered:
            message = f"""
                🚦 Валидатор под мониторингом `{item.pubkey}` должен выпустить блок в слоте {item.slot} 🚦
                (через {f'{round((item.slot - slot)*12/60, 2)} минуты' if (item.slot - slot)*12/60 > 1 else f'{(item.slot - slot)*12} секунд'})"
            """

            if telegram:
                telegram.send_broadcast_message(message)

            print(message)

    return len(filtered)
