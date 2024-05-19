"""Класс для взаимодействия с узлом консенсус слоя"""


import functools
from collections import defaultdict
from functools import lru_cache
from typing import Any, Union

from requests import HTTPError, Response, Session, codes
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ChunkedEncodingError, RetryError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from .data_types import (
    ConsensusClient,
    Block,
    BlockTerm,
    Committees,
    Genesis,
    Header,
    ProposerDuties,
    Rewards,
    Validators,
    ValidatorsLivenessRequestLighthouse,
    ValidatorsLivenessRequestTeku,
    ValidatorsLivenessResponse,
)

from . import config

TIMEOUT_BEACON_SEC = config['TIMEOUT_BEACON_SEC']
StatusType = Validators.DataItem.StatusType


print = functools.partial(print, flush=True)


class NoBlockError(Exception):
    pass


class ConsensusNode:
    """Consensus layer узел (Beacon нода)"""

    def __init__(self, url: str) -> None:
        """Beacon

        Параметры:
        url: Адрес ноды
        """
        self.__url = url
        self.__http_retry_not_found = Session()
        self.__http = Session()
        self.__first_liveness_call = True
        self.__first_rewards_call = True

        adapter_retry_not_found = HTTPAdapter(
            max_retries=Retry(
                backoff_factor=0.5,
                total=3,
                status_forcelist=[
                    codes.not_found,
                    codes.bad_gateway,
                    codes.service_unavailable,
                ],
            )
        )

        adapter = HTTPAdapter(
            max_retries=Retry(
                backoff_factor=0.5,
                total=3,
                status_forcelist=[
                    codes.bad_gateway,
                    codes.service_unavailable,
                ],
            )
        )

        self.__http_retry_not_found.mount("http://", adapter_retry_not_found)
        self.__http_retry_not_found.mount("https://", adapter_retry_not_found)

        self.__http.mount("http://", adapter)
        self.__http.mount("https://", adapter)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(3),
        retry=retry_if_exception_type(ChunkedEncodingError),
    )
    def __get_retry_not_found(self, *args: Any, **kwargs: Any) -> Response:
        """Обёртка над requests.get() с обработкой 404"""
        return self.__http_retry_not_found.get(*args, **kwargs)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(3),
        retry=retry_if_exception_type(ChunkedEncodingError),
    )
    def __get(self, *args: Any, **kwargs: Any) -> Response:
        """Обёртка над requests.get()"""
        return self.__http.get(*args, **kwargs)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(3),
        retry=retry_if_exception_type(ChunkedEncodingError),
    )
    def __post_retry_not_found(self, *args: Any, **kwargs: Any) -> Response:
        """Обёртка над requests.post() с обработкой 404"""
        return self.__http_retry_not_found.post(*args, **kwargs)

    def get_genesis(self) -> Genesis:
        """Получение генезиса"""
        response = self.__get_retry_not_found(
            f"{self.__url}/eth/v1/beacon/genesis", timeout=TIMEOUT_BEACON_SEC
        )
        response.raise_for_status()
        genesis_dict = response.json()
        return Genesis(**genesis_dict)

    def get_header(self, block_identifier: Union[BlockTerm, int]) -> Header:
        """Получение заголовка

        Параметры:
        block_identifier: Идентификатор блока или слот, соответствующий искомому блоку
        """
        try:
            response = self.__get(
                f"{self.__url}/eth/v1/beacon/headers/{block_identifier}", timeout=TIMEOUT_BEACON_SEC
            )

            response.raise_for_status()

        except HTTPError as e:
            if e.response.status_code == codes.not_found:
                # Такой блок не существует
                raise NoBlockError from e

            # Иная ошибка 
            raise

        header_dict = response.json()
        return Header(**header_dict)

    def get_block(self, slot: int) -> Block:
        """Получение блока

        Параметры
        slot: Слот, в который вкладывается блок, который пытаемся получить
        """
        try:
            response = self.__get(
                f"{self.__url}/eth/v2/beacon/blocks/{slot}", timeout=TIMEOUT_BEACON_SEC
            )

            response.raise_for_status()

        except HTTPError as e:
            if e.response.status_code == codes.not_found:
                # Блок не существует
                raise NoBlockError from e

            # Иная ошибка
            raise

        block_dict = response.json()
        return Block(**block_dict)

    @lru_cache()
    def get_proposer_duties(self, epoch: int) -> ProposerDuties:
        """Действия валидатора в эпоху

        epoch: Эпоха, в которую получаем данные о действиях валидатора
        """
        response = self.__get_retry_not_found(
            f"{self.__url}/eth/v1/validator/duties/proposer/{epoch}", timeout=TIMEOUT_BEACON_SEC
        )

        response.raise_for_status()

        proposer_duties_dict = response.json()
        return ProposerDuties(**proposer_duties_dict)

    def get_status2index2validator(
        self,
    ) -> dict[StatusType, dict[int, Validators.DataItem.Validator]]:
        """Формирование вложенного словаря:
        Статус[Индекс валидатора] = Валидатор
        """
        response = self.__get_retry_not_found(
            f"{self.__url}/eth/v1/beacon/states/head/validators", timeout=TIMEOUT_BEACON_SEC
        )

        response.raise_for_status()
        validators_dict = response.json()

        validators = Validators(**validators_dict)

        result: dict[
            StatusType, dict[int, Validators.DataItem.Validator]
        ] = defaultdict(dict)

        for item in validators.data:
            result[item.status][item.index] = item.validator

        return result

    @lru_cache(maxsize=1)
    def get_duty_slot2committee_index2validators_index(
        self, epoch: int
    ) -> dict[int, dict[int, list[int]]]:
        """Формирование вложенного словаря:
        слот[индекс коммитета] = индекс валидатора, который должен аттестовать слот в коммитете

        Параметры:
        epoch: Номер эпохи
        """
        response = self.__get_retry_not_found(
            f"{self.__url}/eth/v1/beacon/states/head/committees",
            params=dict(epoch=epoch),
            timeout=TIMEOUT_BEACON_SEC,
        )

        response.raise_for_status()
        committees_dict = response.json()

        committees = Committees(**committees_dict)
        data = committees.data

        result: dict[int, dict[int, list[int]]] = defaultdict(dict)

        for item in data:
            result[item.slot][item.index] = item.validators

        return result

    def get_rewards(
        self,
        cl_client: ConsensusClient,
        epoch: int,
        validators_index: set[int] | None = None,
    ) -> Rewards:
        """Получение наград за валидацию.

        Параметры:
        cl_client       : Консенсус клиент
        epoch           : Эпоха, за которую нужно получить награды
        validators_index: Множество валидаторов, для которых нужно получить награды.
                          Если отсутсвует - получаем награды для всех валидаторов в сети
        """

        # Для Prysm и Nimbus нет возможности считать награды
        # https://github.com/prysmaticlabs/prysm/issues/11581,
        # https://github.com/status-im/nimbus-eth2/issues/5138,

        if cl_client in {ConsensusClient.NIMBUS, ConsensusClient.PRYSM}:
            if self.__first_rewards_call:
                self.__first_rewards_call = False
                print("⚠️ Используется CL-клиент, для которого нет возможности рассчитать вознаграждения")

            return Rewards(data=Rewards.Data(possible_rewards=[], earned_rewards=[]))

        response = self.__post_retry_not_found(
            f"{self.__url}/eth/v1/beacon/rewards/attestations/{epoch}",
            json=(
                [str(index) for index in sorted(validators_index)]
                if validators_index is not None
                else []
            ),
            timeout=TIMEOUT_BEACON_SEC,
        )

        response.raise_for_status()
        rewards_dict = response.json()
        return Rewards(**rewards_dict)

    def get_validators_liveness(
        self, cl_client: ConsensusClient, epoch: int, validators_index: set[int]
    ) -> dict[int, bool]:
        """Проверка, что валидатор онлайн

        Параметры:
        cl_client       : Тип консенсус узла
        epoch           : Эпоха, для которой нужно проверить что валидатор работает
        validators_index: Валидаторы, которых нужно проверить
        """

        # Для Nimbus считаем, что все валидаторы онлайн
        # https://github.com/status-im/nimbus-eth2/issues/5019,

        if cl_client == ConsensusClient.NIMBUS:
            if self.__first_liveness_call:
                self.__first_liveness_call = False
                print("⚠️ Используется CL-клиент, для которого нет возможности рассчитать пропущенные аттестации")

            return {index: True for index in validators_index}

        cl_client2function = {
            ConsensusClient.OTHER: self.__get_validators_liveness_default,
            ConsensusClient.LIGHTHOUSE: self.__get_validators_liveness_lighthouse,
            ConsensusClient.PRYSM: self.__get_validators_liveness_default,
            ConsensusClient.TEKU: self.__get_validators_liveness_teku
        }

        response = cl_client2function[cl_client](epoch, validators_index)

        try:
            response.raise_for_status()
        except HTTPError as e:
            if e.response.status_code != codes.bad_request:
                raise
            
            # Запрашиваемая эпоха была очень давно или сервис только-только начал работу,
            print(
                f'❓     Для эпохи {epoch} невозможно определение пропущенных аттестаций.'
            )

            print(
                f'''❓     Игнорируйте это сообщение, если сервис начал работу менее
                "одной эпохи назад. В противном случае проверьте, что используется корректный
                "параметр --cl-client (сейчас установлен `{cl_client}`). '''
            )

            print("❓     Используйте `--help` для подробностей.")

            return {index: True for index in validators_index}

        validators_liveness_dict = response.json()
        validators_liveness = ValidatorsLivenessResponse(**validators_liveness_dict)

        return {item.index: item.is_live for item in validators_liveness.data}

    def get_potential_block(self, slot) -> Block | None:
        """Получение блока, если возможно, иначе - None.

        Параметры:
        slot: Слот, относящийся к блоку, о котором нужно получить данные.
        """
        try:
            return self.get_block(slot)
        except NoBlockError:
            # Блок вероятно orphaned - добыт и подтвержден, но не включен в основную цепочку.
            # Бикончейн видит блок (поэтому было получено событие)
            return None

    # https://github.com/sigp/lighthouse/issues/4243
    def __get_validators_liveness_lighthouse(
        self, epoch: int, validators_index: set[int]
    ) -> Response:
        """Получить статус активных валидаторов от Lighthouse.

        Параметры:
        epoch           : Эпоха, для которой нужно получить данные
        validators_index: Множество индексов валидаторов, для которых нужно получить данные
        """
        return self.__post_retry_not_found(
            f"{self.__url}/lighthouse/liveness",
            json=ValidatorsLivenessRequestLighthouse(
                epoch=epoch, indices=sorted(list(validators_index))
            ).model_dump(),
            timeout=TIMEOUT_BEACON_SEC,
        )

    # https://github.com/ConsenSys/teku/issues/7204
    def __get_validators_liveness_teku(
        self, epoch: int, validators_index: set[int]
    ) -> Response:
        """Получить статус активных валидаторов от Teku.

        Параметры:
        epoch           : Эпоха, для которой нужно получить данные
        validators_index: Множество индексов валидаторов, для которых нужно получить данные
        """
        return self.__post_retry_not_found(
            f"{self.__url}/eth/v1/validator/liveness/{epoch}",
            json=ValidatorsLivenessRequestTeku(
                indices=sorted(list(validators_index))
            ).model_dump(),
            timeout=TIMEOUT_BEACON_SEC,
        )
    
    def __get_validators_liveness_default(
        self, epoch: int, validators_index: set[int]
    ) -> Response:
        """Статус активных валидаторов для других клиентов

        Параметры:
        epoch           : Эпоха, для которой нужно получить данные
        validators_index: Множество индексов валидаторов, для которых нужно получить данные
        """
        return self.__post_retry_not_found(
            f"{self.__url}/eth/v1/validator/liveness/{epoch}",
            json=[
                str(validator_index)
                for validator_index in sorted(list(validators_index))
            ],
            timeout=TIMEOUT_BEACON_SEC,
        )
