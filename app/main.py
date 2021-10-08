import os
from typing import Dict
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from uvicorn import Config, Server
from dataclasses import dataclass
from enum import Enum

PORT = int(os.getenv("PORT", "8000"))
app = FastAPI()


class ResolverBody(BaseModel):
    operacao: str
    arguments: Dict[str, str]


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
        self.tipo_de_eleicao_ativa = "ring"

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


glInfo = InfoBody()
glPeers = [
    {
        "id": "201720295",
        "nome": "Allana Dos Santos Campos",
        "url": "https://sd-ascampos-20212.herokuapp.com/",
    },
    {
        "id": "201512136",
        "nome": "Annya Rita De Souza Ourives",
        "url": "https://sd-annyaourives-20212.herokuapp.com/hello",
    },
    {
        "id": "201710375",
        "nome": "Emmanuel Norberto Ribeiro Dos Santos",
        "url": "https://sd-emmanuel.herokuapp.com/",
    },
    {
        "id": "201710376",
        "nome": "Guilherme Senna Cruz",
        "url": "https://nodejs-sd-guilhermesenna.herokuapp.com/",
    },
    {
        "id": "201710377",
        "nome": "Hiago Rios Cordeiro",
        "url": "https://sd-api-uesc.herokuapp.com/",
    },
    {
        "id": "201810665",
        "nome": "Jenilson Ramos Santos",
        "url": "https://jenilsonramos-sd-20211.herokuapp.com/",
    },
    {
        "id": "201610327",
        "nome": "João Pedro De Gois Pinto",
        "url": "https://sd-joaopedrop-20212.herokuapp.com/",
    },
    {
        "id": "201610337",
        "nome": "Luís Carlos Santos Câmara",
        "url": "https://sd-20212-luiscarlos.herokuapp.com/",
    },
    {
        "id": "201620400",
        "nome": "Nassim Maron Rihan",
        "url": "https://sd-nassimrihan-2021-2.herokuapp.com/",
    },
    {
        "id": "201710396",
        "nome": "Robert Morais Santos Broketa",
        "url": "https://pratica-sd.herokuapp.com/",
    },
    {
        "id": "201720308",
        "nome": "Victor Dos Santos Santana",
        "url": "https://sd-victor-20212.herokuapp.com/",
    },
]


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


@app.get("/info")
def get_info():
    return glInfo


@app.put("/info")
def update_info(body: InfoBody):
    if any(not att or (type(att) is float and att <= 0.0) for att in body.get_atts()):
        raise HTTPException(
            status_code=400, detail="A requisição não contem os dados necessários"
        )
    else:
        global glInfo
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
        if nome.lower() == "robert":
            return {"url": "https://pratica-sd.herokuapp.com/"}


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


if __name__ == "__main__":
    main()
