import os
from typing import Dict
from pydantic import BaseModel
from fastapi import FastAPI
from uvicorn import Config, Server

PORT = int(os.getenv("PORT", "8000"))
app = FastAPI()


class Body(BaseModel):
    operacao: str
    arguments: Dict[str, str]


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


@app.post("/resolver")
def resolver(body: Body):
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
