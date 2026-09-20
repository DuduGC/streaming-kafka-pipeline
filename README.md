# Streaming Kafka Pipeline

Este repositório contém um MVP de pipeline de eventos para uma plataforma de reprodução de vídeo ou áudio. O fluxo registra eventos como `PLAY`, `PAUSE`, `BUFFERING`, `ERROR` e `COMPLETE`, transporta cada evento pelo Kafka e grava o resultado no PostgreSQL.

O projeto foi montado para praticar os fundamentos de streaming que aparecem em sistemas reais: comunicação assíncrona, particionamento, consumer groups, confirmação de offsets, persistência idempotente, execução em containers e controle de privilégios.

**Producer → Kafka → Consumer → PostgreSQL**

## Resultado

O ambiente executa quatro serviços Docker:

- `producer`, que gera eventos sintéticos e os publica;
- `kafka`, que transporta os eventos;
- `consumer`, que valida, persiste e confirma o processamento;
- `postgres`, que armazena os eventos processados.

As decisões principais são:

- topic `playback-events` com três partitions;
- message key `content_id`;
- consumer group `playback-events-consumer`;
- entrega at-least-once com commit manual;
- persistência idempotente por `event_id`;
- usuário PostgreSQL restrito para o Consumer;
- 41 testes locais e três testes de integração Docker aprovados.

## Arquitetura

~~~mermaid
flowchart LR
    P[Producer Python] -->|JSON + content_id| K[Kafka<br/>playback-events<br/>3 partitions]
    K -->|consumer group| C[Consumer Python]
    C -->|SQLAlchemy| DB[(PostgreSQL<br/>playback_events)]
~~~

### Responsabilidade de cada serviço

| Serviço | Responsabilidade |
|---|---|
| Producer | Gera eventos sintéticos, serializa JSON, garante o topic e publica no Kafka. |
| Kafka | Transporta os eventos, distribui por partitions e mantém offsets. |
| Consumer | Consome, desserializa, valida, persiste e confirma offsets. |
| PostgreSQL | Armazena eventos processados e impede duplicação por chave primária. |

Os serviços compartilham a rede bridge `pipeline`. Dentro dela, o Producer e o Consumer usam `kafka:9092` e `postgres:5432`. `localhost` não serve para comunicação entre containers, pois aponta para o próprio container.

## O que foi implementado em cada etapa

| Etapa | Entrega |
|---:|---|
| 1 | Arquitetura, estrutura inicial e responsabilidades dos quatro serviços. |
| 2 | Docker Compose com Kafka em KRaft, PostgreSQL, rede, volume e healthchecks. |
| 3 | Producer Python containerizado, eventos sintéticos, UUID, JSON, key e publicação idempotente do topic. |
| 4 | Consumer Python containerizado, polling, desserialização, grupo, shutdown e commit manual. |
| 5 | PostgreSQL com tabela `playback_events`, SQLAlchemy e persistência parametrizada. |
| 6 | Validação do contrato, descarte consciente de mensagens inválidas e idempotência por `event_id`. |
| 7 | Testes unitários e integração real Producer → Kafka → Consumer → PostgreSQL. |
| 8 | Revisão de partitions, key, provisionamento explícito, readiness, rede e offsets. |
| 9 | Revisão de segurança: entrada, secrets, logs, containers e menor privilégio no PostgreSQL. |
| 10 | README final e verificação end-to-end do sistema completo. |

## Execução rápida

Pré-requisito: Docker Desktop com Docker Compose.

1. Crie o arquivo de configuração local:

~~~powershell
Copy-Item .env.example .env
notepad .env
~~~

Troque as senhas de exemplo. `POSTGRES_PASSWORD` e `POSTGRES_APP_PASSWORD` devem ser diferentes.

2. Construa e inicie os quatro serviços:

~~~powershell
docker compose up -d --build
docker compose ps
~~~

3. Acompanhe a publicação e o consumo:

~~~powershell
docker compose logs -f producer consumer
~~~

4. Consulte as últimas linhas persistidas:

~~~powershell
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT event_id, user_id, content_id, event_type, event_timestamp, processed_at FROM playback_events ORDER BY processed_at DESC LIMIT 5;"'
~~~

5. Pare os containers preservando o volume:

~~~powershell
docker compose down
~~~

O volume `postgres_data` mantém os dados entre reinicializações. `docker compose down -v` remove o volume e todos os eventos, portanto deve ser usado apenas quando a intenção for começar do zero.

## Contrato do evento

~~~json
{
  "event_id": "5b834643-48c7-4b40-8a72-e6a3453e58b8",
  "user_id": "user_123",
  "content_id": "serie_42",
  "event_type": "PLAY",
  "timestamp": "2026-09-06T15:00:00Z"
}
~~~

Os tipos aceitos são `PLAY`, `PAUSE`, `BUFFERING`, `ERROR` e `COMPLETE`. O Producer gera apenas dados sintéticos.

Antes de acessar o banco, o Consumer valida:

- JSON UTF-8 de até 10.000 bytes;
- os cinco campos obrigatórios;
- strings não vazias e tipos básicos;
- `event_id` como UUID;
- `event_type` dentro do conjunto permitido;
- timestamp ISO-8601 em UTC;
- `user_id` e `content_id` com até 100 caracteres;
- ausência de NUL e de surrogates Unicode isolados.

Quando uma mensagem falha nessas regras, o sistema registra o motivo sem armazenar o payload, descarta a mensagem e confirma o offset. O MVP não possui DLQ.

## Kafka

| Conceito | Decisão do projeto |
|---|---|
| Broker | Um broker Apache Kafka 4.2.1 em modo KRaft, sem ZooKeeper. |
| Topic | `playback-events`, criado explicitamente pelo Producer. |
| Partitions | Três, suficientes para demonstrar distribuição e ordenação. |
| Replication factor | Um, compatível com o broker único local. |
| Message key | `content_id`, mantendo eventos do mesmo conteúdo na mesma partition. |
| Consumer group | `playback-events-consumer`. |
| Offset | Confirmado manualmente depois de um resultado definitivo. |
| Serialização | JSON UTF-8. |

O Producer usa `acks=all`, idempotência do cliente e `AdminClient` para criar ou validar o topic. A criação automática indiscriminada fica desabilitada.

### At-least-once e idempotência

O Consumer desativa auto-commit e auto-store. Ele persiste o evento primeiro e confirma o offset depois. Se o processo cair nesse intervalo, Kafka pode entregar a mensagem novamente. Esse é o comportamento esperado da semântica at-least-once.

`event_id` é a chave primária da tabela. O insert usa `ON CONFLICT (event_id) DO NOTHING`, por isso uma reentrega vira uma duplicata reconhecida e não cria outra linha.

Para inspecionar o topic e o lag:

~~~powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic playback-events
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group playback-events-consumer
~~~

## Docker

- Imagens oficiais fixadas: `apache/kafka:4.2.1`, `postgres:17.11-alpine3.24` e `python:3.13.15-slim-bookworm`.
- Producer e Consumer usam Dockerfiles próprios, dependências controladas e usuário non-root UID 10001.
- Kafka e PostgreSQL possuem healthchecks.
- `depends_on` inicia Producer e Consumer somente depois que as dependências ficam saudáveis.
- PostgreSQL usa o volume nomeado `postgres_data`.
- Nenhuma porta é publicada no host; a operação ocorre na rede interna.
- `.dockerignore` exclui secrets, caches, testes e artefatos do contexto de build.
- O script de criação do papel da aplicação é montado como somente leitura.

Healthcheck informa se um serviço está disponível naquele momento. Ele não substitui o tratamento de erros: o Producer repete a conexão com Kafka e o Consumer reposiciona a partition quando uma falha transitória impede a persistência.

## PostgreSQL

A tabela `playback_events` contém:

| Coluna | Regra |
|---|---|
| `event_id` | UUID, chave primária e proteção contra duplicação. |
| `user_id` | Identificador sintético, `VARCHAR(100)`. |
| `content_id` | Conteúdo sintético, `VARCHAR(100)`. |
| `event_type` | Tipo permitido do evento. |
| `event_timestamp` | Timestamp UTC produzido na origem. |
| `processed_at` | Timestamp gerado pelo banco na persistência. |

O acesso usa SQLAlchemy 2.0 e parâmetros vinculados. Nenhum payload é concatenado em SQL.

O PostgreSQL separa as identidades:

- `POSTGRES_USER` e `POSTGRES_PASSWORD`: bootstrap administrativo do banco;
- `POSTGRES_APP_USER` e `POSTGRES_APP_PASSWORD`: papel de runtime do Consumer.

O papel do Consumer recebe somente conexão no banco, uso do schema, `INSERT` na tabela e leitura de `event_id`, necessária para resolver `ON CONFLICT (event_id)`. O script `database/002-create-app-role.sh` é idempotente e também pode migrar um volume já existente sem apagar dados.

## Configuração

Copie `.env.example` para `.env`. O arquivo `.env` é ignorado e não deve ser enviado ao GitHub.

| Variável | Usada por | Finalidade |
|---|---|---|
| `POSTGRES_DB` | PostgreSQL, Consumer | Banco de eventos. |
| `POSTGRES_USER` | PostgreSQL | Usuário administrativo de bootstrap. |
| `POSTGRES_PASSWORD` | PostgreSQL | Senha administrativa local. |
| `POSTGRES_APP_USER` | PostgreSQL, Consumer | Usuário restrito da aplicação. |
| `POSTGRES_APP_PASSWORD` | PostgreSQL, Consumer | Senha exclusiva da aplicação. |
| `POSTGRES_HOST` / `POSTGRES_PORT` | Consumer | `postgres:5432` na rede Docker. |
| `KAFKA_BOOTSTRAP_SERVERS` | Producer, Consumer | `kafka:9092` na rede Docker. |
| `KAFKA_TOPIC` | Producer, Consumer | `playback-events`. |
| `KAFKA_TOPIC_PARTITIONS` | Producer | Três partitions esperadas. |
| `KAFKA_CONSUMER_GROUP` | Consumer | Grupo de consumo. |
| `PRODUCER_INTERVAL_SECONDS` | Producer | Intervalo entre eventos. |
| `LOG_LEVEL` | Producer, Consumer | Nível de logging. |

As configurações essenciais usam validação fail-fast. Para um volume antigo que ainda não possui o papel restrito:

~~~powershell
docker compose exec -T postgres sh /docker-entrypoint-initdb.d/002-create-app-role.sh
docker compose up -d --build consumer
~~~

## Segurança: riscos e controles

| Risco combatido | Controle implementado |
|---|---|
| Senha ou secret exposto no código | Credenciais ficam em variáveis de ambiente; `.env` é ignorado e `.env.example` usa placeholders. |
| Comprometimento do Consumer com privilégio de cluster | Bootstrap administrativo separado do papel de runtime, que não possui `SUPERUSER`, `CREATEDB`, `CREATEROLE` ou `REPLICATION`. |
| Mensagem Kafka venenosa bloqueando uma partition | Limites de tamanho, tipos, UUID, NUL e Unicode inválido são verificados antes do banco; mensagens determinísticas inválidas são descartadas e confirmadas. |
| SQL injection ou alteração de query por payload | SQLAlchemy usa parâmetros vinculados; não há concatenação de valores de eventos. |
| Container Python com privilégios do host | Producer e Consumer executam como usuário non-root. |
| Exposição acidental de broker ou banco | Nenhuma porta é publicada no host; serviços usam somente a rede interna. |
| Vazamento através de logs | Logs registram metadados operacionais, sem payload completo, senha ou connection string. |
| Build contaminado por arquivos locais | `.dockerignore` exclui `.env`, caches, IDEs, logs, testes e artefatos. |
| Dependência de serviço ainda indisponível | Healthchecks e `depends_on` condicionado à saúde das dependências. |

## Logging

O código usa o módulo Python `logging`, nunca `print()`, para registrar:

- conexão e shutdown;
- publicação com topic, partition e offset;
- recebimento e persistência por `event_id`;
- duplicatas ignoradas;
- mensagens inválidas;
- falhas de conexão ou persistência.

Exemplos:

~~~powershell
docker compose logs --tail=50 producer
docker compose logs --tail=50 consumer
~~~

## Testes e evidências

### Suíte local

A execução normal não depende de Python no host; Python e Pytest locais são usados apenas para desenvolvimento e testes.

~~~powershell
python -m pytest tests -q
~~~

Resultado final: **41 testes aprovados**. A suíte cobre geração, serialização, configuração Kafka, validação, commits, persistência, falhas antes do commit, idempotência, limites de entrada e configuração do Consumer.

### Integração Docker

Com os quatro serviços ativos:

~~~powershell
docker compose up -d --build
$env:RUN_DOCKER_INTEGRATION = "1"
python -m pytest tests/test_pipeline_integration.py -v -m integration
Remove-Item Env:RUN_DOCKER_INTEGRATION
~~~

Resultado final: **3 testes aprovados**:

1. Producer real → Kafka → Consumer → PostgreSQL, com todos os campos consultados no banco.
2. Criação, descrição e remoção de um topic temporário com três partitions.
3. Verificação dos privilégios efetivos do papel usado pelo Consumer.

## Estrutura do projeto

~~~text
.
├── producer/
│   ├── event_generator.py
│   ├── producer.py
│   └── Dockerfile
├── consumer/
│   ├── consumer.py
│   ├── database.py
│   ├── validator.py
│   ├── requirements.txt
│   └── Dockerfile
├── database/
│   ├── init.sql
│   └── 002-create-app-role.sh
├── tests/
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
├── .env.example
├── .gitignore
├── .dockerignore
└── README.md
~~~

## Status final

As dez etapas foram concluídas:

1. Arquitetura e estrutura mínima.
2. Docker Compose.
3. Producer.
4. Consumer.
5. PostgreSQL.
6. Validação e idempotência.
7. Testes.
8. Revisão Docker/Kafka.
9. Revisão de segurança.
10. README e verificação end-to-end.

Estado comprovado: **41 testes locais aprovados, três integrações Docker aprovadas e fluxo completo operacional em containers**.
