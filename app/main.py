import os
import json
from fastapi import FastAPI
from uvicorn import Config, Server

PORT = int(os.getenv("PORT", "8000"))
app = FastAPI()


@app.get("/")
def index():
    return {
        "routes": {
            "GET": {
                "/": "This page",
                "/itens": "List of fruits",
                "/clients": "List of Clients"
            },
            "POST": {
                "/echo": "Echoes the passed parameter"
            }

        }
    }


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
