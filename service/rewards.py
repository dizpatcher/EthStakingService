"""Получение наград валидаторов"""

from typing import Tuple

from prometheus_client import Counter, Gauge

from service.utils import LimitedDict

from .cl_node import ConsensusNode
from .data_types import ConsensusClient, Validators
from .notifier import Telegram

Validator = Validators.DataItem.Validator

Reward = Tuple[int, int, int]  # source, target, head
AreRewardsMaximum = Tuple[bool, bool, bool]  # source, target, head

# Вся сеть
# ------------------
(
    prom_network_source_rate_gauge,
    prom_network_target_rate_gauge,
    prom_network_head_rate_gauge,
) = (
    Gauge("network_source_rate", "Source rate сети"),
    Gauge("network_target_rate", "Target rate сети"),
    Gauge("network_head_rate", "Head rate сети"),
)

(
    prom_network_possible_source_rewards_count,
    prom_network_possible_target_rewards_count,
    prom_network_possible_head_rewards_count,
) = (
    Counter("network_possible_sources", "Количество корректных аттестаций source"),
    Counter("network_possible_targets", "Количество корректных аттестаций target"),
    Counter("network_possible_heads", "Количество корректных аттестаций head"),
)

(
    prom_network_earned_source_rewards_count,
    prom_network_penalties_sources_count,
    prom_network_earned_target_rewards_count,
    prom_network_penalties_targets_count,
    prom_network_earned_head_rewards_count,
) = (
    Counter("network_rewards_source", "Количество вознагражденных аттестаций source"),
    Counter("network_penalties_source", "Количество убыточных аттестаций source"),
    Counter("network_rewards_target", "Количество вознагражденных аттестаций target"),
    Counter("network_penalties_target", "Количество убыточных аттестаций target"),
    Counter("network_rewards_head", "Количество аттестаций head"),
)

# Валидаторы под мониторингом (из файла)
# --------------
(
    prom_source_rate_gauge,
    prom_target_rate_gauge,
    prom_head_rate_gauge,
) = (
    Gauge("own_source_rate", "Source rate (выбранные валидаторы)"),
    Gauge("own_target_rate", "Target rate (выбранные валидаторы)"),
    Gauge("own_head_rate", "Head rate (выбранные валидаторы)"),
)

(
    prom_possible_source_rewards_count,
    prom_possible_target_rewards_count,
    prom_possible_head_rewards_count,
) = (
    Counter("own_possible_sources", "Количество корректных аттестаций source (выбранные)"),
    Counter("own_possible_targets", "Количество корректных аттестаций target (выбранные)"),
    Counter("own_possible_heads", "Количество корректных аттестаций head (выбранные)"),
)

(
    prom_earned_source_rewards_count,
    prom_penalties_sources_count,
    prom_earned_target_rewards_count,
    prom_penalties_targets_count,
    prom_earned_head_rewards_count,
) = (
    Counter("own_rewards_source", "Вознаграждения за source-аттестации (выбранные валидаторы)"),
    Counter("own_penalties_source", "Штрафы за source-аттестации (выбранные валидаторы)"),
    Counter("own_rewards_target", "Вознаграждения за target-аттестации (выбранные валидаторы)"),
    Counter("own_penalties_target", "Штрафы за target-аттестации (выбранные валидаторы)"),
    Counter("own_rewards_head", "Вознаграждения за head-аттестации (выбранные валидаторы)"),
)


def _log(
    pubkeys: Tuple[str],
    are_rewards_maximum: Tuple[bool],
    performance_metric: float,
    epoch: int,
    picto: str,
    label: str,
    telegram: Telegram | None,
) -> None:
    
    not_perfect_pubkeys = {
        pubkey for (pubkey, perfect) in zip(pubkeys, are_rewards_maximum) if not perfect
    }

    if len(not_perfect_pubkeys) > 0:
        first_not_perfect_pubkeys = sorted(not_perfect_pubkeys)[:5]

        first_not_perfect_pubkeys = [
            pubkey[:10] for pubkey in first_not_perfect_pubkeys
        ]

        n_extra_vals = len(not_perfect_pubkeys) - len(first_not_perfect_pubkeys)
        message = f"""
            {picto} Валидаторы {', '.join(first_not_perfect_pubkeys)}{f' и ещё {n_extra_vals} ' if n_extra_vals > 0 else ''} упустили вознаграждение за {label}-аттестацию  в эпоху {epoch-2} 
            ({label} rate = {performance_metric:.2%})
        """

        if telegram is not None:
            telegram.send_broadcast_message(message)

        print(message)


def process_rewards(
    cl_node: ConsensusNode,
    cl_client: ConsensusClient,
    epoch: int,
    net_epoch2index2validator: LimitedDict,
    our_epoch2index2validator: LimitedDict,
    telegram: Telegram | None,
) -> None:
    """Обработка ревардов в эпоху для валидаторов

    Параметры:
        cl_node: Beacon-нода
        cl_client: CL-клиент
        epoch: эпоха

        net_epoch2index2validator : Словарь:
            эпоха[индекс[валидатор]]

        our_epoch2index2validator : Словарь:
            эпоха[индекс[валидатор]]
    """

    if epoch < 2:
        return

    # Валидаторы сети
    # ------------------
    net_index2validator = (
        net_epoch2index2validator[epoch - 2]
        if epoch - 2 in net_epoch2index2validator
        else (
            net_epoch2index2validator[epoch - 1]
            if epoch - 1 in net_epoch2index2validator
            else net_epoch2index2validator[epoch]
        )
    )

    if len(net_index2validator) == 0:
        return

    data = cl_node.get_rewards(cl_client, epoch - 2).data

    effective_balance2possible_reward: dict[int, Reward] = {
        reward.effective_balance: (reward.source, reward.target, reward.head)
        for reward in data.ideal_rewards
    }

    index2earned_reward: dict[int, Reward] = {
        reward.validator_index: (reward.source, reward.target, reward.head)
        for reward in data.total_rewards
    }

    items = [
        _compare_rewards(
            validator.pubkey,
            effective_balance2possible_reward[validator.effective_balance],
            index2earned_reward[index],
        )
        for index, validator in net_index2validator.items()
        if index in index2earned_reward
    ]

    unzipped: Tuple[ Tuple[str], Tuple[Reward], Tuple[Reward], Tuple[AreRewardsMaximum] ] = zip(*items)

    _, possible_rewards, earned_rewards, are_rewards_maximum = unzipped

    possible_sources, possible_targets, possible_heads = zip(*possible_rewards)
    earned_sources, earned_targets, earned_heads = zip(*earned_rewards)
    are_sources_max, are_targets_max, are_heads_max = zip(*are_rewards_maximum)

    total_possible_sources = sum(possible_sources)
    total_possible_targets = sum(possible_targets)
    total_possible_heads = sum(possible_heads)

    prom_network_possible_source_rewards_count.inc(total_possible_sources)
    prom_network_possible_target_rewards_count.inc(total_possible_targets)
    prom_network_possible_head_rewards_count.inc(total_possible_heads)

    total_earned_sources = sum(earned_sources)
    total_earned_targets = sum(earned_targets)
    total_earned_heads = sum(earned_heads)

    (
        prom_network_earned_source_rewards_count
        if total_earned_sources >= 0
        else prom_network_penalties_sources_count
    ).inc(abs(total_earned_sources))

    (
        prom_network_earned_target_rewards_count
        if total_earned_targets >= 0
        else prom_network_penalties_targets_count
    ).inc(abs(total_earned_targets))

    prom_network_earned_head_rewards_count.inc(total_earned_heads)

    source_rate = sum(are_sources_max) / len(are_sources_max)
    target_rate = sum(are_targets_max) / len(are_targets_max)
    head_rate = sum(are_heads_max) / len(are_heads_max)

    prom_network_source_rate_gauge.set(source_rate)
    prom_network_target_rate_gauge.set(target_rate)
    prom_network_head_rate_gauge.set(head_rate)

    # Отслеживаемые валидаторы
    # --------------
    our_index2validator = (
        our_epoch2index2validator[epoch - 2]
        if epoch - 2 in our_epoch2index2validator
        else (
            our_epoch2index2validator[epoch - 1]
            if epoch - 1 in our_epoch2index2validator
            else our_epoch2index2validator[epoch]
        )
    )

    our_indexes = set(our_index2validator)

    if len(our_indexes) == 0:
        return

    data = cl_node.get_rewards(cl_client, epoch - 2, our_indexes).data

    effective_balance2possible_reward = {
        reward.effective_balance: (reward.source, reward.target, reward.head)
        for reward in data.ideal_rewards
    }

    index2earned_reward = {
        reward.validator_index: (reward.source, reward.target, reward.head)
        for reward in data.total_rewards
    }

    items = [
        _compare_rewards(
            validator.pubkey,
            effective_balance2possible_reward[validator.effective_balance],
            index2earned_reward[index],
        )
        for index, validator in our_index2validator.items()
    ]

    unzipped = zip(*items)

    pubkeys, possible_rewards, earned_rewards, are_rewards_maximum = unzipped

    possible_sources, possible_targets, possible_heads = zip(*possible_rewards)
    earned_sources, earned_targets, earned_heads = zip(*earned_rewards)
    are_sources_max, are_targets_max, are_heads_max = zip(*are_rewards_maximum)

    total_possible_sources = sum(possible_sources)
    total_possible_targets = sum(possible_targets)
    total_possible_heads = sum(possible_heads)

    prom_possible_source_rewards_count.inc(total_possible_sources)
    prom_possible_target_rewards_count.inc(total_possible_targets)
    prom_possible_head_rewards_count.inc(total_possible_heads)

    total_earned_sources = sum(earned_sources)
    total_earned_targets = sum(earned_targets)
    total_earned_heads = sum(earned_heads)

    (
        prom_earned_source_rewards_count
        if total_earned_sources >= 0
        else prom_penalties_sources_count
    ).inc(abs(total_earned_sources))

    (
        prom_earned_target_rewards_count
        if total_earned_targets >= 0
        else prom_penalties_targets_count
    ).inc(abs(total_earned_targets))

    prom_earned_head_rewards_count.inc(total_earned_heads)

    source_rate = sum(are_sources_max) / len(are_sources_max)
    target_rate = sum(are_targets_max) / len(are_targets_max)
    head_rate = sum(are_heads_max) / len(are_heads_max)

    prom_source_rate_gauge.set(source_rate)
    prom_target_rate_gauge.set(target_rate)
    prom_head_rate_gauge.set(head_rate)

    _log(pubkeys, are_sources_max, source_rate, epoch, '🚰', 'source', telegram)
    _log(pubkeys, are_targets_max, target_rate, epoch, '🎯', 'target', telegram)
    _log(pubkeys, are_heads_max, head_rate, epoch, '👤', 'head', telegram)


def _compare_rewards(
    pubkey: str,
    possible_reward: Reward,
    earned_reward: Reward,
) -> Tuple[str, Reward, Reward, AreRewardsMaximum]:
    
    (possible_source_reward, possible_target_reward, possible_head_reward) = possible_reward
    (earned_source_reward, earned_target_reward, actual_head_reward) = earned_reward

    are_rewards_maximum = (
        earned_source_reward == possible_source_reward,
        earned_target_reward == possible_target_reward,
        actual_head_reward == possible_head_reward,
    )

    return pubkey, possible_reward, earned_reward, are_rewards_maximum
