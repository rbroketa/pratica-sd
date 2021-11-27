import functools
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Iterator, Optional
from uuid import UUID, uuid4

import requests
from fastapi import FastAPI, HTTPException
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from uvicorn import Config, Server
import asyncio
from contextlib import suppress

PORT = int(os.getenv("PORT", "8000"))
app = FastAPI()


class ResolverBody(BaseModel):
    operacao: str
    arguments: Dict[str, str]


@dataclass
class RecursoBody:
    codigo_de_acesso: str
    _uuid: Optional[UUID] = None
    valor: Optional[int] = None
    validade: Optional[datetime] = None
    # constructor

    def __init__(self, codigo_de_acesso: UUID, valor: int, validade: datetime) -> None:
        self._uuid = codigo_de_acesso
        self.valor = valor
        self.validade = validade


EXPIRACAO = 5


@dataclass
class InfoBody:
    server_name: str
    server_endpoint: str
    descricao: str
    versao: float
    status: str
    tipo_de_eleicao_ativa: str

    def __init__(self) -> None:
        self.server_name = "sd_microservice"
        self.server_endpoint = "https://pratica-sd.herokuapp.com/"
        self.descricao = "Projeto de SD. Os seguintes serviços estão implementados. GET: [/, /info, /peers, /peers/{id}, /fruits, /clients]. POST: [/resolver, /peers, /echo]. PUT: [/info, /peers/{id}]. DELETE: [/peer/{id}]"
        self.versao = 0.1
        self.status = "online"
        self.tipo_de_eleicao_ativa = "anel"

    def get_atts(self):
        return [
            self.server_name,
            self.server_endpoint,
            self.descricao,
            self.versao,
            self.status,
            self.tipo_de_eleicao_ativa,
        ]


@dataclass
class Valid(Enum):
    VALID = 0b00
    INVALID = 0b01
    DUPLICATE = 0b10


class EleicaoBody(BaseModel):
    id: str
    dados: Optional[list[str]] = None


@dataclass
class CoordenadorBody:
    coordenador: str
    id_eleicao: str


recursos: dict[UUID, RecursoBody] = {}

glInfo = InfoBody()
glPeers = [
    # {
    #     "id": "201810665",
    #     "nome": "Jenilson Ramos Santos",
    #     "url": "https://jenilsonramos-sd-20211.herokuapp.com/",
    # },
    # {
    #     "id": "201720308",
    #     "nome": "Victor Dos Santos Santana",
    #     "url": "https://sd-victor-20212.herokuapp.com/",
    # },
    {
        "id": "201720295",
        "nome": "Allana Dos Santos Campos",
        "url": "https://sd-ascampos-20212.herokuapp.com/",
    },
    {
        "id": "201710396",
        "nome": "Robert Morais Santos Broketa",
        "url": "https://pratica-sd.herokuapp.com/",
    },
    {
        "id": "201710377",
        "nome": "Hiago Rios Cordeiro",
        "url": "https://sd-api-uesc.herokuapp.com/",
    },
    {
        "id": "201710376",
        "nome": "Guilherme Senna Cruz",
        "url": "https://nodejs-sd-guilhermesenna.herokuapp.com/",
    },
    # {
    #     "id": "201710375",
    #     "nome": "Emmanuel Norberto Ribeiro Dos Santos",
    #     "url": "https://sd-emmanuel.herokuapp.com/",
    # },
    # {
    #     "id": "201620400",
    #     "nome": "Nassim Maron Rihan",
    #     "url": "https://sd-nassimrihan-2021-2.herokuapp.com/",
    # },
    # {
    #     "id": "201610337",
    #     "nome": "Luís Carlos Santos Câmara",
    #     "url": "https://sd-20212-luiscarlos.herokuapp.com/",
    # },
    # {
    #     "id": "201610327",
    #     "nome": "João Pedro De Gois Pinto",
    #     "url": "https://sd-joaopedrop-20212.herokuapp.com/",
    # },
    # {
    #     "id": "201512136",
    #     "nome": "Annya Rita De Souza Ourives",
    #     "url": "https://sd-annyaourives-20212.herokuapp.com/hello/",
    # },
]

myUrl = "https://pratica-sd.herokuapp.com/"
myId = "201710396"
coordenador = {
    "coordenador": False,
    "coordenador_atual": "",
}
id_eleicao_atual = ""
eleicoes: set[str] = set()
interval_check = 2.0


def is_peer_valid(peer: dict[str, str]) -> Valid:
    for (k, v) in peer.items():
        if k != "id" and k != "nome" and k != "url":
            return Valid.INVALID

        if k == "id" or k == "nome" or k == "url":
            if k == "nome" and v.isdigit():
                return Valid.INVALID
            if k == "url" and not v.startswith("http"):
                return Valid.INVALID
    try:
        glPeers.index(peer)
        return Valid.DUPLICATE
    except ValueError:
        return Valid.VALID


@app.get("/")
def index():
    return {
        "routes": {
            "GET": {
                "/": "This page",
                "/fruits": "List of fruits",
                "/clients": "List of Clients",
            },
            "POST": {
                "/echo": "Echoes the passed parameter",
                "/resolver": {
                    "body": {
                        "resolver": "operacao",
                        "nome": "name of the person to match a service url",
                    },
                    "response": {"url": "url of the service of the matched name"},
                },
            },
        },
    }


def recurso_expirou(validade: datetime) -> bool:
    return datetime.now() - validade >= timedelta(seconds=EXPIRACAO)


def log(sev: str, comment: str, msg: str):
    requests.post(
        "https://sd-log-server.herokuapp.com/log",
        json={
            "from": myUrl,
            "severity": sev,
            "comment": comment,
            "body": msg,
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    print(f"OMG! The client sent invalid data!: {exc.body}")
    return await request_validation_exception_handler(request, exc)


@app.put("/tipo_eleicao")
def update_tipo_eleicao(tipo_eleicao: str):
    global glInfo
    glInfo.tipo_de_eleicao_ativa = tipo_eleicao
    return {"tipo_eleicao": tipo_eleicao}


@app.post("/offline/{body}")
def post_offline(body: bool = True):
    global glInfo
    if body:
        glInfo.status = "offline"
    else:
        glInfo.status = "online"

    return {"status": glInfo.status}


@app.get("/eleicao")
def get_eleicao():
    return {
        "tipo_de_eleicao_ativa": glInfo.tipo_de_eleicao_ativa,
        "eleicoes_em_andamento": eleicoes,
    }


@app.post("/eleicao")
def post_eleicao(body: EleicaoBody):
    if glInfo.status == "offline":
        raise HTTPException(status_code=404, detail="Servidor offline")

    global coordenador

    if glInfo.status == "online" and (
        glInfo.tipo_de_eleicao_ativa == "anel" or not eleicoes.__contains__(body.id)
    ):
        eleicoes.add(body.id)
        threading.Thread(target=eleicao, args=(body.id, body.dados, True)).start()
    return body.dict()


@app.post("/eleicao/coordenador")
def post_eleicao_coordenador(body: CoordenadorBody):
    if glInfo.status == "offline":
        raise HTTPException(status_code=404, detail="Servidor offline")
    global coordenador

    coordenador["coordenador_atual"] = body.coordenador
    coordenador["coordenador"] = body.coordenador == myId
    log(
        "Success",
        f"Eleicao finalizada",
        f"Eleicao: {body.id_eleicao}. Novo coordenador: {body.coordenador}",
    )
    print(f"Eleicao {body.id_eleicao} finalizada. Novo coordenador: {body.coordenador}")
    eleicoes.discard(body.id_eleicao)


@app.post("/resetar")
def resetar_coord():
    global coordenador
    coordenador["coordenador"] = False
    coordenador["coordenador_atual"] = ""
    eleicoes.clear()
    return {"status": "ok"}


@app.get("/recurso")
def get_recurso(body: RecursoBody):
    try:
        recurso = recursos.get(UUID(body.codigo_de_acesso))
    except ValueError:
        raise HTTPException(status_code=401, detail="Chave inválida")
    if recurso is None or recurso_expirou(recurso.validade):
        raise HTTPException(
            status_code=401, detail="Recurso expirado ou não encontrado"
        )
    return {"valor": recurso.valor}


@app.delete("/recurso")
def delete_recurso(body: RecursoBody):
    recurso = recursos.get(UUID(body.codigo_de_acesso))
    if recurso is None:
        raise HTTPException(status_code=410, detail="Recurso não existe")

    if recurso._uuid is not None:
        del recursos[recurso._uuid]

    if recurso_expirou(recurso.validade):
        raise HTTPException(status_code=410, detail="Recurso expirado")


@app.put("/recurso")
def put_recurso(body: RecursoBody):
    recurso = recursos.get(UUID(body.codigo_de_acesso))
    if recurso is None or recurso_expirou(recurso.validade):
        raise HTTPException(
            status_code=401, detail="Recurso expirado ou não encontrado"
        )
    recurso.valor = body.valor
    recurso.validade = datetime.now()

    if recurso._uuid is not None:
        recursos[recurso._uuid] = recurso

    return {"codigo_de_acesso": recurso._uuid, "valor": recurso.valor}


@app.post("/recurso")
def post_recurso(body: RecursoBody = None):
    if body is None:
        uid = uuid4()
        validade = datetime.now() + timedelta(seconds=EXPIRACAO)
        recursos[uid] = RecursoBody(
            codigo_de_acesso=uid,
            valor=random.randint(1, 1000),
            validade=validade,
        )
        return {"codigo_de_acesso": uid, "validade": validade}
    else:
        # get recurso from dict
        recurso = recursos.get(UUID(body.codigo_de_acesso))
        # raise exception with code 409 if recurso is not None and recurso
        # validade is now plus EXPIRACAO seconds
        if recurso is not None:
            if recurso.validade is not None and not recurso_expirou(recurso.validade):
                recurso.validade = datetime.now()

                if recurso._uuid is not None:
                    recursos[recurso._uuid] = recurso
                raise HTTPException(status_code=409, detail="Recurso em uso")

            if body.valor is not None:
                recurso.valor = body.valor
            # update recurso validade
            recurso.validade = datetime.now()
            # update dict
            if recurso._uuid is not None:
                recursos[recurso._uuid] = recurso
            return {
                "codigo_de_acesso": recurso._uuid,
                "validade": recurso.validade,
            }
        else:
            uid = uuid4()
            validade = datetime.now() + timedelta(seconds=EXPIRACAO)
            recursos[uid] = RecursoBody(
                codigo_de_acesso=uid,
                valor=random.randint(1, 1000),
                validade=validade,
            )
            return {"codigo_de_acesso": uid, "validade": validade}


@app.get("/info")
def get_info():
    return glInfo


@app.put("/info")
def update_info(body: InfoBody):
    if any(
        not att or (isinstance(att, float) and att <= 0.0) for att in body.get_atts()
    ):
        raise HTTPException(
            status_code=400, detail="A requisição não contem os dados necessários"
        )
    else:
        global glInfo
        if glInfo.status == "offline" and body.status == "online":
            threading.Thread(target=eleicao, args=("")).start()

        glInfo = body


@app.get("/peers")
def get_peers():
    return glPeers


@app.get("/peers/{id}")
def get_peer(id: str):
    for d in glPeers:
        if d.get("id") == id:
            return d
    raise HTTPException(404, f"Não encontrado peer com id: {id}")


@app.post("/peers")
def add_peer(body: dict[str, str]):
    valid = is_peer_valid(body)
    if valid.value == Valid.VALID.value:
        glPeers.append(body)
    elif valid.value == Valid.INVALID.value:
        raise HTTPException(400, "Dados mal formatados")
    else:
        raise HTTPException(409, "Já existe um peer com esse id ou nome")


@app.put("/peers/{id}")
def update_peer(id: str, body: dict[str, str]):
    if is_peer_valid(body).value == Valid.INVALID.value:
        raise HTTPException(422, f"Dados invalidos")

    for d in glPeers:
        if d.get("id") == id:
            d.update(body)
            return body
    raise HTTPException(404, f"Não encontrado peer com id: {id}")


@app.delete("/peers/{id}")
def delete_peer(id: str):
    idx = -1
    for (i, d) in enumerate(glPeers):
        if d.get("id") == id:
            idx = i
            break
    if idx == -1:
        raise HTTPException(404, f"Não encontrado peer com id: {id}")
    glPeers.pop(idx)


@app.post("/resolver")
def resolver(body: ResolverBody):
    if body.operacao == "resolver":
        nome = body.arguments.get("nome")
        if nome is not None and nome.lower() == "robert":
            return {"url": "https://pratica-sd.herokuapp.com/"}


@app.get("/coordenador")
def get_coordenador():
    return coordenador


@app.get("/fruits")
def app_get():
    return ["Apple", "Banana", "Orange"]


@app.get("/clients")
def app_clientes_get():
    return ["Mathias", "José", "Thiago"]


@app.post("/echo")
def app_post(echo=None):
    if echo is None:
        return "Echo."
    else:
        f"Echo {echo}."


def main():
    config = Config(app=app, host="0.0.0.0", port=PORT, debug=True)
    server = Server(config=config)
    server.run()

    global glInfo
    if glInfo.status != "offline":
        log(
            "Attention",
            "Servico iniciado",
            "O servico foi inicializado, uma eleicao ira ocorrer em breve",
        )
        eleicao("")
    # setInterval(interval_check, check_coordenador)
    p = Periodic(check_coordenador, 2)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(p.start())


def eleicao(
    id_eleicao_atual: str, dados: Optional[list[str]] = None, recebido: bool = False
):
    if glInfo.status == "offline":
        return
    if id_eleicao_atual == "":
        id_eleicao_atual = uuid4().__str__()
        eleicoes.add(id_eleicao_atual)
    log(
        "Success",
        f"{'Recebido' if recebido else 'Iniciado nova'} eleicao",
        f"Eleicao: {id_eleicao_atual}. Tipo de eleicao: {glInfo.tipo_de_eleicao_ativa}",
    )
    print(
        f"Iniciando nova eleicao: {id_eleicao_atual}. Tipo: {glInfo.tipo_de_eleicao_ativa}"
    )
    if glInfo.tipo_de_eleicao_ativa == "anel":
        eleicao_ring(id_eleicao_atual, dados)
    else:
        eleicao_bully(id_eleicao_atual)


def cycle(iterable: Iterator):
    it = iter(iterable)
    while True:
        v = next(it)
        if v.get("id") == myId:
            break

    while True:
        try:
            yield next(it)
        except StopIteration:
            it = iter(iterable)
            yield next(it)


def eleicao_ring(id_eleicao_atual: str, dados: Optional[list[str]] = None):
    global glPeers

    print(f"dados: {dados}")
    if dados is not None and dados.__contains__(myId):
        maxId = functools.reduce(
            lambda acc, id: max(int(acc), int(id) if id != "" else 0), dados, 0
        )
        print("maxId: ", maxId)
        end_election(id_eleicao_atual, str(maxId))
    else:
        if dados is None:
            dados = [myId]
        else:
            dados.append(myId)

        peers = glPeers.copy()
        peers.sort(key=lambda x: int(x.get("id")))
        print(peers)

        for peer in cycle(peers):
            # if peer["id"] == myId:
            #     continue

            res = requests.post(
                f"{peer['url']}eleicao", json={"id": id_eleicao_atual, "dados": dados}
            )

            if res.status_code == 200:
                print(f"Enviado eleicao para {peer['id']}({peer['nome']})")
                break
            else:
                log(
                    "Error",
                    f"Erro ao enviar dados para eleicao a {peer['url']}",
                    f"Status code: {res.status_code}",
                )
                print(f"Erro ao enviar dados para eleicao a {peer['url']}")


def eleicao_bully(id_eleicao_atual: str):
    res_count = 0

    peers = glPeers.copy()
    peers.sort(key=lambda x: int(x.get("id")))
    print(peers)

    for peer in peers:
        if peer["id"] == myId:
            continue
        elif int(peer["id"]) < int(myId):
            break
        else:
            res = requests.post(
                f"{peer['url']}eleicao",
                json={"id": id_eleicao_atual, "dados": []},
            )
            if res.status_code == 200:
                print(f"Enviado eleicao para {peer['id']}({peer['nome']})")
                res_count += 1
            else:
                log(
                    "Error",
                    f"Erro ao enviar dados para eleicao a {peer['url']}",
                    f"Status code: {res.status_code}",
                )
                print(f"Erro ao enviar dados para eleicao a {peer['url']}")

    if res_count == 0:
        end_election(id_eleicao_atual, myId)


def end_election(id_eleicao_atual: str, id: str):
    message = (
        "Look at me! I'm the boss now"
        if id == myId
        else f"Novo coordenador eleito: {id}"
    )

    log("Success", f"Eleicao {id_eleicao_atual} finalizada. Coordenador: {id}", message)
    print(f"Eleicao {id_eleicao_atual} finalizada. Coordenador: {id}")
    for peer in glPeers:
        if peer["id"] == myId:
            continue

        requests.post(
            f"{peer['url']}eleicao/coordenador",
            json={"id_eleicao": id_eleicao_atual, "coordenador": id},
        )

    coordenador.update(
        {
            "coordenador": id == myId,
            "coordenador_atual": id,
        }
    )

    eleicoes.discard(id_eleicao_atual)


class Periodic:
    def __init__(self, func, time):
        self.func = func
        self.time = time
        self.is_started = False
        self._task = None

    async def start(self):
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            await asyncio.sleep(self.time)
            self.func()


def check_coordenador():
    if glInfo.status == "online":
        if coordenador.get("coordenador_atual") == "":
            log("Attention", "Coordenador nao identificado", "Iniciando nova eleicao")
            eleicao("")
            return

        if coordenador.get("coordenador") is False:
            for peer in glPeers:
                if peer.get("id") == coordenador.get("coordenador_atual"):
                    res = requests.get(f"{peer.get('url')}info")

                    if res.status_code != 200 or res.json().get("status") == "offline":
                        n = random.randint(5, 10)
                        time.sleep(n)
                        log("Attention", "Coordenador offline", "Iniciando nova eleicao")
                        eleicao("")


if __name__ == "__main__":
    main()
