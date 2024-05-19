"""Класс для взаимодейством с узлом уровня исполнения"""


from requests import Session, codes
from requests.adapters import HTTPAdapter, Retry

from .data_types import EthGetBlockByHashRequest, ExecutionBlock


class ExecutionNode:
    """Узел уровня исполнения"""

    def __init__(self, url: str) -> None:
        """Execution-нода

        Параметры:
        url: Адрес ноды
        """
        self.__url = url
        self.__http = Session()

        adapter = HTTPAdapter(
            max_retries=Retry(
                backoff_factor=0.5,
                total=3,
                status_forcelist=[codes.not_found],
            )
        )

        self.__http.mount("http://", adapter)
        self.__http.mount("https://", adapter)

    def eth_get_block_by_hash(self, hash: str) -> ExecutionBlock:
        """Получение блока с уровня исполнения.

        Параметры:
        hash: Хэш блока, который необходимо получить
        """
        request_body = EthGetBlockByHashRequest(params=[hash, True])
        # print('request_body', request_body)
        # print('EL node url', self.__url)
        response = self.__http.post(self.__url, json=request_body.model_dump())
        response.raise_for_status()
        execution_block_dict = response.json()
        print(execution_block_dict)
        return
        return ExecutionBlock(**execution_block_dict)
