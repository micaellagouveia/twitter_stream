# twitter_stream

## Parte 1
- [X] Conectar-se com a API do twiiter e fazer o stream das mensagens
- [X] Produzir as mensagens usando Kafka
- [X] Consumir e tratar as mensagens, mandando para um novo tópico no Kafka
- [] Consumir novo tópico usando Spark


## Como rodar

### Criando ambiente e instalando as dependências
- ```python -m venv env```
- ```source env/bin/activate```
- ```pip install -r src/requirements.txt```

### Rodando os containers
- ```docker compose up```
- ```docker compose up producer```
- ```docker compose up consumer```

- Fazer a consulta pelo Kafdrop na porta ```localhost:19000```