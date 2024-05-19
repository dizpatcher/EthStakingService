"""Точка входа для CLI."""

import math
import functools
from . import config
from os import environ
from pathlib import Path
from time import sleep, time
from typing import List, Optional

import typer
from prometheus_client import Gauge, start_http_server
from typer import Option

from .cl_node import ConsensusNode
from .el_node import ExecutionNode
from .coingecko import Coingecko
from .notifier import Telegram
from .activation_queue import export_activation_duration
from .attestations import (
    process_paired_missed_attestations,
    process_missed_attestations,
)
from .blocks import (
    process_block_proposal,
    process_missed_blocks_finalized, 
    process_missed_blocks_head, 
    process_future_blocks_proposal
)
from .data_types import ConsensusClient, Validators
from .rewards import process_rewards
from .validators import ValidatorsSlashed, ValidatorsExited
from .attestations import process_attestations
from .utils import (
    LimitedDict,
    lower_eth1_address,
    get_own_pubkeys,
    get_next_slot
)
from . import config
from .notifier import Telegram
from misc import EL_NODE_URL

print = functools.partial(print, flush=True)
app = typer.Typer(add_completion=False)

SECONDS_PER_SLOT =  config['SECONDS_PER_SLOT']
SLOTS_PER_EPOCH = config['SLOTS_PER_EPOCH']

MISSED_BLOCK_TIMEOUT_SEC = config['MISSED_BLOCK_TIMEOUT_SEC']
SLOT_FOR_MISSED_ATTESTATIONS_PROCESS = config['SLOT_FOR_MISSED_ATTESTATIONS_PROCESS']
SLOT_FOR_REWARDS_PROCESS = config['SLOT_FOR_REWARDS_PROCESS']

Status = Validators.DataItem.StatusType

prom_slot_gauge = Gauge("slot", "Слот")
prom_epoch_gauge = Gauge("epoch", "Эпоха")

prom_queued_validators_gauge = Gauge(
    "own_validators_pending",
    "Количество ожидающих в очереди валидаторов под мониторингом",
)

prom_network_queued_validators_gauge = Gauge(
    "network_validators_pending",
    "Общее количество валидаторов в очереди",
)

prom_active_validators_gauge = Gauge(
    "own_validators_active",
    "Количество активных валидаторов под мониторингом",
)

prom_network_active_validators_gauge = Gauge(
    "network_validators_active",
    "Всего активных валидаторов в сети",
)


@app.command()
def run(
    cl_node: str = Option(..., help="URL коненсус узла сети", show_default=False),
    el_node: str = Option(None, help="URL узла исполнения в сети", show_default=False),
    pubkeys_file: Optional[Path] = Option(
        None,
        help="Файл, содержащий список ключей валидатора для мониторинга",
        exists=True,
        file_okay=True,
        dir_okay=False,
        show_default=False,
    ),
    cl_client: ConsensusClient = Option(
        ConsensusClient.OTHER,
        help=(
                "Указание используемого на ноде Consensus-клиент (Lighthouse, Prysm, Teku, Nimbus, other)"
        ),
        show_default=True,
    )
) -> None:
    """
    🚨 Сервис мониторинга стейкинга в Ethereum 🚨

    \b
    Сервис мониторинга стейкинга в Ethereum в реальном времени отслеживает консенсус слой Ethereum и уведомляет вас, если валидаторы:
    - назначены для предложения блока в следующие 2 эпохи
    - не смогли создать блок
    - не смогли провести полную аттестацию
    - пропустили аттестацию
    - пропустили две аттестации подряд
    - покинули сеть
    - получили штраф (слэшинг)
    - создали блок от неизвестного релея
    - получили не полные награды за аттестации

    \b
    Также предусмотрен импорт таких базовых вещей как:
    - объём в долларах, находящихся под мониторингом
    - рыночная капитализация всего рынка стейкинга Ethereum
    - текущую эпоху и слот в сети
    - общее число "заслешенных" валидаторов
    - обменные курс ETH/USD, ETH/RUB
    - валидаторы под мониторингом в очереди на активацию
    - активные валидаторы под мониторингом
    - вышедшие валидаторы под мониторингом
    - количество валидаторов в очереди на активацию в сети
    - количество активных валидаторов в сети
    - ожидаемое время в очереди на активацию

    \b
    Дополнительно, можно указать следующие параметры:
    - путь к файлу, содержащий публичные ключи валидаторов, которые необходимо отслеживать

    \b
    Ключи валидаторов динамически подгружабтся в начале каждой эпохи.
    - Если ключи указаны в файлы, можно редактировать его без необходимости перезагрузки сервиса.

    \b
    Сервис экспортирует описанные выще данные в логи, Prometheus и Telegram

    Prometheus автоматически запущен на 8000 порту.
    """
    try: 
        handle(
            cl_node,
            el_node,
            pubkeys_file,
            cl_client
        )
    except KeyboardInterrupt: 
        print("⛔     Мониторинг остановлен.")


def handle(
    cl_node_url: str,
    el_node_url: str | None,
    pubkeys_file: Path | None,
    cl_client: ConsensusClient | None,
) -> None:
    """Просто оболочка, позволяющая протестировать функцию обработчика"""
    print('Запуск сервиса...')

    cl_node = ConsensusNode(cl_node_url)
    el_node = ExecutionNode(el_node_url) if el_node_url is not None else None
    coingecko = Coingecko()
    telegram = Telegram()

    own_pubkeys: set[str] = set()
    own_active_idx2val: dict[int, Validators.DataItem.Validator] = {}
    own_validators_missed_attestation: set[int] = set()
    own_validators_missed_previous_attestation: set[int] = set()
    own_epoch2active_idx2val = LimitedDict(3)
    net_epoch2active_idx2val = LimitedDict(3)

    exited_validators = ValidatorsExited(telegram)
    slashed_validators = ValidatorsSlashed(telegram)

    last_missed_attestations_process_epoch: int | None = None
    last_rewards_process_epoch: int | None = None

    previous_epoch: int | None = None
    last_processed_finalized_slot: int | None = None

    genesis = cl_node.get_genesis()

    for monitoring_slot_number, (slot, slot_start_time_sec) in enumerate(get_next_slot(genesis.data.genesis_time)):
        if slot < 0:
            seconds2start = - slot * SECONDS_PER_SLOT
            print(f"⏱️  Оставшееся время: {slot} слотов ({seconds2start} секунд)")
            continue

        epoch = math.floor(slot / SLOTS_PER_EPOCH)
        slot_in_epoch = slot % SLOTS_PER_EPOCH

        prom_slot_gauge.set(slot)
        prom_epoch_gauge.set(epoch)

        is_new_epoch = previous_epoch is None or previous_epoch != epoch

        if last_processed_finalized_slot is None:
            last_processed_finalized_slot = slot

        if is_new_epoch:
            try:
                own_pubkeys = get_own_pubkeys(pubkeys_file)
            except ValueError:
                raise typer.BadParameter("Некоторые ключи валидаторов не валидны")

            # Валидаторы сети
            # ------------------
            net_status2idx2val = cl_node.get_status2index2validator()

            net_pending_q_idx2val = net_status2idx2val.get(Status.pending_queued, {})
            n_pending_validators = len(net_pending_q_idx2val)
            prom_network_queued_validators_gauge.set(n_pending_validators)

            active_ongoing = net_status2idx2val.get(Status.active_ongoing, {})
            active_exiting = net_status2idx2val.get(Status.active_exiting, {})
            active_slashed = net_status2idx2val.get(Status.active_slashed, {})
            net_active_idx2val = active_ongoing | active_exiting | active_slashed
            net_epoch2active_idx2val[epoch] = net_active_idx2val

            n_active_validators = len(net_active_idx2val)
            prom_network_active_validators_gauge.set(n_active_validators)

            net_exited_s_idx2val = net_status2idx2val.get(Status.exited_slashed, {})

            withdrawal_possible = net_status2idx2val.get(Status.withdrawal_possible, {})
            withdrawal_done = net_status2idx2val.get(Status.withdrawal_done, {})
            net_withdrawable_idx2val = withdrawal_possible | withdrawal_done

            # Валидаторы для собственного мониторинга
            # --------------
            own_status2idx2val = {
                status: {
                    index: validator
                    for index, validator in validator.items()
                    if validator.pubkey in own_pubkeys
                }
                for status, validator in net_status2idx2val.items()
            }

            own_queued_idx2val = own_status2idx2val.get(Status.pending_queued, {})
            prom_queued_validators_gauge.set(len(own_queued_idx2val))

            ongoing = own_status2idx2val.get(Status.active_ongoing, {})
            active_exiting = own_status2idx2val.get(Status.active_exiting, {})
            active_slashed = own_status2idx2val.get(Status.active_slashed, {})
            own_active_idx2val = ongoing | active_exiting | active_slashed
            own_epoch2active_idx2val[epoch] = own_active_idx2val

            prom_active_validators_gauge.set(len(own_active_idx2val))
            own_exited_u_idx2val = own_status2idx2val.get(Status.exited_unslashed, {})
            own_exited_s_idx2val = own_status2idx2val.get(Status.exited_slashed, {})

            withdrawal_possible = own_status2idx2val.get(Status.withdrawal_possible, {})
            withdrawal_done = own_status2idx2val.get(Status.withdrawal_done, {})
            own_withdrawable_idx2val = withdrawal_possible | withdrawal_done

            exited_validators.process(own_exited_u_idx2val, own_withdrawable_idx2val)

            slashed_validators.process(
                net_exited_s_idx2val,
                own_exited_s_idx2val,
                net_withdrawable_idx2val,
                own_withdrawable_idx2val,
            )

            export_activation_duration(n_active_validators, n_pending_validators)
            coingecko.get_eth_exchange_rate('usd')
            coingecko.get_eth_exchange_rate('rub')

        if previous_epoch is not None and previous_epoch != epoch:
            message = f"🏁     Эпоха     {epoch}     началась."
            print(message)
            # telegram.send_broadcast_message(message)

        is_process_missed_attestations = (
            slot_in_epoch >= SLOT_FOR_MISSED_ATTESTATIONS_PROCESS
            and (
                last_missed_attestations_process_epoch is None
                or last_missed_attestations_process_epoch != epoch
            )
        )

        if is_process_missed_attestations:
            own_validators_missed_attestation = (
                process_missed_attestations(
                    cl_node, cl_client, own_epoch2active_idx2val, epoch, telegram
                )
            )

            process_paired_missed_attestations(
                own_validators_missed_attestation,
                own_validators_missed_previous_attestation,
                epoch,
                telegram,
            )

            last_missed_attestations_process_epoch = epoch

        is_slot_big_enough = slot_in_epoch >= SLOT_FOR_REWARDS_PROCESS
        is_last_rewards_epoch_none = last_rewards_process_epoch is None
        is_new_rewards_epoch = last_rewards_process_epoch != epoch
        epoch_condition = is_last_rewards_epoch_none or is_new_rewards_epoch
        should_process_rewards = is_slot_big_enough and epoch_condition

        if should_process_rewards:
            process_rewards(
                cl_node,
                cl_client,
                epoch,
                net_epoch2active_idx2val,
                own_epoch2active_idx2val,
                telegram
            )

            last_rewards_process_epoch = epoch

        process_future_blocks_proposal(cl_node, own_pubkeys, slot, is_new_epoch, telegram)

        block_reward = process_block_proposal(last_processed_finalized_slot)
        last_processed_finalized_slot = process_missed_blocks_finalized(
            cl_node, last_processed_finalized_slot, slot, block_reward, own_pubkeys, telegram
        )

        delta_sec = MISSED_BLOCK_TIMEOUT_SEC - (time() - slot_start_time_sec)
        sleep(max(0, delta_sec))

        potential_block = cl_node.get_potential_block(slot)

        if potential_block is not None:
            block = potential_block
            
            # block_hash = block.data.message.body.execution_payload.block_hash
            # el_node.eth_get_block_by_hash(block_hash)
            process_attestations(
                cl_node,
                block,
                slot,
                own_active_idx2val,
                telegram
            )

        is_own_validator = process_missed_blocks_head(
            cl_node,
            potential_block,
            slot,
            own_pubkeys,
            telegram
        )

        # if is_own_validator and potential_block is not None:
        #     pass

        own_validators_missed_previous_attestation = own_validators_missed_attestation
        previous_epoch = epoch

        if slot_in_epoch >= SLOT_FOR_MISSED_ATTESTATIONS_PROCESS:
            is_process_missed_attestations = True

        if monitoring_slot_number == 0:
            start_http_server(8000)
