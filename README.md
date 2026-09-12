# Streaming Kafka Pipeline

Pipeline de eventos de reprodução MVP. O projeto demonstra, de forma executável, como Docker Compose, Apache Kafka, Python e PostgreSQL trabalham juntos em um fluxo assíncrono:

**Producer → Kafka → Consumer → PostgreSQL**

O objetivo é mostrar fundamentos básicos de arquitetura de eventos que aprendi recentemente.

## Resultado

- Quatro serviços Docker: <code>producer</code>, <code>kafka</code>, <code>consumer</code> e <code>postgres</code>.
- Topic <code>playback-events</code> com três partitions.
- Message key <code>content_id</code>.
- Consumer group <code>playback-events-consumer</code>.
- Entrega at-least-once com commit manual.
- Persistência idempotente por <code>event_id</code>.
- Usuário PostgreSQL restrito para o Consumer.
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

Os serviços usam a rede bridge <code>pipeline</code>. Dentro dela, o Producer e o Consumer acessam <code>kafka:9092</code> e <code>postgres:5432</code>. <code>localhost</code> não é usado para comunicação entre containers, porque aponta para o próprio container.

## O que foi implementado em cada etapa

| Etapa | Entrega |
|---:|---|
| 1 | Arquitetura, estrutura inicial e responsabilidades dos quatro serviços. |
| 2 | Docker Compose com Kafka em KRaft, PostgreSQL, rede, volume e healthchecks. |
| 3 | Producer Python containerizado, eventos sintéticos, UUID, JSON, key e publicação idempotente do topic. |
| 4 | Consumer Python containerizado, polling, desserialização, grupo, shutdown e commit manual. |
| 5 | PostgreSQL com tabela <code>playback_events</code>, SQLAlchemy e persistência parametrizada. |
| 6 | Validação do contrato, descarte consciente de mensagens inválidas e idempotência por <code>event_id</code>. |
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

Substitua as senhas de exemplo. <code>POSTGRES_PASSWORD</code> e <code>POSTGRES_APP_PASSWORD</code> devem ser diferentes.

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

O volume <code>postgres_data</code> mantém os dados entre reinicializações. <code>docker compose down -v</code> remove o volume e todos os eventos; use somente quando quiser começar do zero.

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

Tipos aceitos: <code>PLAY</code>, <code>PAUSE</code>, <code>BUFFERING</code>, <code>ERROR</code> e <code>COMPLETE</code>. Todos os dados gerados pelo Producer são sintéticos.

O Consumer valida:

- JSON UTF-8 de até 10.000 bytes;
- os cinco campos obrigatórios;
- strings não vazias e tipos básicos;
- <code>event_id</code> como UUID;
- <code>event_type</code> dentro do conjunto permitido;
- timestamp ISO-8601 em UTC;
- <code>user_id</code> e <code>content_id</code> com até 100 caracteres;
- ausência de NUL e de surrogates Unicode isolados.

Uma mensagem inválida é registrada sem o payload, descartada conscientemente e confirmada. Não há DLQ neste MVP.

## Kafka

| Conceito | Decisão do projeto |
|---|---|
| Broker | Um broker Apache Kafka 4.2.1 em modo KRaft, sem ZooKeeper. |
| Topic | <code>playback-events</code>, criado explicitamente pelo Producer. |
| Partitions | Três, suficientes para demonstrar distribuição e ordenação. |
| Replication factor | Um, compatível com o broker único local. |
| Message key | <code>content_id</code>, mantendo eventos do mesmo conteúdo na mesma partition. |
| Consumer group | <code>playback-events-consumer</code>. |
| Offset | Confirmado manualmente depois de um resultado definitivo. |
| Serialização | JSON UTF-8. |

O Producer usa <code>acks=all</code>, idempotência do cliente e <code>AdminClient</code> para criar ou validar o topic. A criação automática indiscriminada está desabilitada.

### At-least-once e idempotência

O Consumer desativa auto-commit e auto-store. Primeiro persiste o evento; depois confirma o offset. Se o processo falhar entre essas duas ações, Kafka pode entregar a mesma mensagem novamente — essa é a semântica at-least-once.

<code>event_id</code> é a chave primária da tabela. O insert usa <code>ON CONFLICT (event_id) DO NOTHING</code>, então a reentrega é reconhecida como duplicata e não cria outra linha.

Para inspecionar o topic e o lag:

~~~powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic playback-events
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group playback-events-consumer
~~~

## Docker

- Imagens oficiais fixadas: <code>apache/kafka:4.2.1</code>, <code>postgres:17.11-alpine3.24</code> e <code>python:3.13.15-slim-bookworm</code>.
- Producer e Consumer usam Dockerfiles próprios, dependências controladas e usuário non-root UID 10001.
- Kafka e PostgreSQL possuem healthchecks.
- <code>depends_on</code> inicia Producer e Consumer somente após as dependências estarem saudáveis.
- PostgreSQL usa o volume nomeado <code>postgres_data</code>.
- Nenhuma porta é publicada no host; a operação ocorre na rede interna.
- <code>.dockerignore</code> exclui secrets, caches, testes e artefatos do contexto de build.
- O script de criação do papel da aplicação é montado como somente leitura.

Healthcheck é uma verificação de disponibilidade, não um substituto para tratamento de erros: o Producer repete conexão com Kafka e o Consumer reposiciona a partition em falhas transitórias de persistência.

## PostgreSQL

A tabela <code>playback_events</code> contém:

| Coluna | Regra |
|---|---|
| <code>event_id</code> | UUID, chave primária e proteção contra duplicação. |
| <code>user_id</code> | Identificador sintético, <code>VARCHAR(100)</code>. |
| <code>content_id</code> | Conteúdo sintético, <code>VARCHAR(100)</code>. |
| <code>event_type</code> | Tipo permitido do evento. |
| <code>event_timestamp</code> | Timestamp UTC produzido na origem. |
| <code>processed_at</code> | Timestamp gerado pelo banco na persistência. |

O acesso é feito com SQLAlchemy 2.0 e parâmetros vinculados. Nenhum payload é concatenado em SQL.

O PostgreSQL separa as identidades:

- <code>POSTGRES_USER</code> e <code>POSTGRES_PASSWORD</code>: bootstrap administrativo do banco;
- <code>POSTGRES_APP_USER</code> e <code>POSTGRES_APP_PASSWORD</code>: papel de runtime do Consumer.

O papel do Consumer recebe somente conexão no banco, uso do schema, <code>INSERT</code> na tabela e leitura de <code>event_id</code>, necessária para resolver <code>ON CONFLICT (event_id)</code>. O script <code>database/002-create-app-role.sh</code> é idempotente e também pode migrar um volume já existente sem apagar dados.

## Configuração

Copie <code>.env.example</code> para <code>.env</code>. O arquivo <code>.env</code> é ignorado e não deve ser enviado ao GitHub.

| Variável | Usada por | Finalidade |
|---|---|---|
| <code>POSTGRES_DB</code> | PostgreSQL, Consumer | Banco de eventos. |
| <code>POSTGRES_USER</code> | PostgreSQL | Usuário administrativo de bootstrap. |
| <code>POSTGRES_PASSWORD</code> | PostgreSQL | Senha administrativa local. |
| <code>POSTGRES_APP_USER</code> | PostgreSQL, Consumer | Usuário restrito da aplicação. |
| <code>POSTGRES_APP_PASSWORD</code> | PostgreSQL, Consumer | Senha exclusiva da aplicação. |
| <code>POSTGRES_HOST</code> / <code>POSTGRES_PORT</code> | Consumer | <code>postgres:5432</code> na rede Docker. |
| <code>KAFKA_BOOTSTRAP_SERVERS</code> | Producer, Consumer | <code>kafka:9092</code> na rede Docker. |
| <code>KAFKA_TOPIC</code> | Producer, Consumer | <code>playback-events</code>. |
| <code>KAFKA_TOPIC_PARTITIONS</code> | Producer | Três partitions esperadas. |
| <code>KAFKA_CONSUMER_GROUP</code> | Consumer | Grupo de consumo. |
| <code>PRODUCER_INTERVAL_SECONDS</code> | Producer | Intervalo entre eventos. |
| <code>LOG_LEVEL</code> | Producer, Consumer | Nível de logging. |

Configurações essenciais usam validação fail-fast. Para um volume antigo que ainda não possui o papel restrito:

~~~powershell
docker compose exec -T postgres sh /docker-entrypoint-initdb.d/002-create-app-role.sh
docker compose up -d --build consumer
~~~

## Segurança: riscos e controles

| Risco combatido | Controle implementado |
|---|---|
| Senha ou secret exposto no código | Credenciais ficam em variáveis de ambiente; <code>.env</code> é ignorado e <code>.env.example</code> usa placeholders. |
| Comprometimento do Consumer com privilégio de cluster | Bootstrap administrativo separado do papel de runtime, que não possui <code>SUPERUSER</code>, <code>CREATEDB</code>, <code>CREATEROLE</code> ou <code>REPLICATION</code>. |
| Mensagem Kafka venenosa bloqueando uma partition | Limites de tamanho, tipos, UUID, NUL e Unicode inválido são verificados antes do banco; mensagens determinísticas inválidas são descartadas e confirmadas. |
| SQL injection ou alteração de query por payload | SQLAlchemy usa parâmetros vinculados; não há concatenação de valores de eventos. |
| Container Python com privilégios do host | Producer e Consumer executam como usuário non-root. |
| Exposição acidental de broker ou banco | Nenhuma porta é publicada no host; serviços usam somente a rede interna. |
| Vazamento através de logs | Logs registram metadados operacionais, sem payload completo, senha ou connection string. |
| Build contaminado por arquivos locais | <code>.dockerignore</code> exclui <code>.env</code>, caches, IDEs, logs, testes e artefatos. |
| Dependência de serviço ainda indisponível | Healthchecks e <code>depends_on</code> condicionado à saúde das dependências. |

## Logging

O código usa o módulo Python <code>logging</code>, nunca <code>print()</code>, para registrar:

- conexão e shutdown;
- publicação com topic, partition e offset;
- recebimento e persistência por <code>event_id</code>;
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

Todas as dez etapas foram concluídas:

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
