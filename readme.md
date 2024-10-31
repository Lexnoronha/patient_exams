
# Processa dados JSON para PostgresSQL.

Este projeto realiza a consolidação e análise de dados a partir de arquivos JSON e gera resultados em arquivos CSV. Utilizamos o Airflow para orquestração dos fluxos de trabalho e o PostgreSQL para armazenamento dos dados.

## Estrutura do Projeto

- `config/`: Configurações específicas para o projeto (exemplo: conexões, variáveis do Airflow).
- `dags/`: Diretório onde as DAGs do Airflow são definidas.
  - `core/`: Contém scripts principais para manipulação e transformação dos dados.
    - `raw/`: Dados brutos em formato JSON.
      - `caduceus.consolidation_anonymized.json`: Arquivo JSON com os dados brutos anonimizados.
    - `resultados/`: Resultados das transformações em formato CSV.
      - `clients_total.csv`
      - `pat_chronic_conditions.csv`
      - `pat_complex_conditions.csv`
      - `pat_conditions.csv`
      - `pat_medications.csv`
      - `pat_total.csv`
    - Scripts Python para processamento de cada arquivo nas Dags.
- `logs/`: Diretório onde os logs do Airflow são armazenados.
- `plugins/`: Plugins personalizados do Airflow.
- `.env`: Arquivo de variáveis de ambiente.
- `.gitignore`: Arquivo para ignorar arquivos e diretórios no controle de versão.
- `docker-compose.yaml`: Configuração do Docker Compose para subir os serviços do Airflow e PostgreSQL.
- `Dockerfile`: Dockerfile para configurar o ambiente do Airflow.
- `readme.md`: Documentação do projeto.
- `requirements.txt`: Arquivo com as dependências do projeto.

## Pré-requisitos

- [Docker](https://www.docker.com/get-started) e [Docker Compose](https://docs.docker.com/compose/) instalados.

## Configuração

1. **Instalação de Dependências**: As dependências necessárias estão listadas no arquivo `requirements.txt` e serão instaladas automaticamente no container.

## Executando o Projeto

1. **Subir os Serviços**: Para iniciar o Airflow e o PostgreSQL, execute o comando:
   ```bash
   docker-compose up -d
   ```
   Isso iniciará o Airflow e o PostgreSQL em containers Docker.

2. **Acessando o Airflow**: 
   - O Airflow estará disponível em [http://localhost:8080](http://localhost:8080).
   - Use as credenciais padrão (definidas no `docker-compose.yaml`) para fazer login.
    ```bash
   user: airflow
   password: airflow
   ```
   - Caso ocorra um erro de conexão com o Postgres, verificar as credenciais do banco (definidas no `docker-compose.yaml`) para o IP do container do Postgres.

3. **Executando as DAGs**: As DAGs no diretório `dags/` contêm os fluxos de trabalho para processar e consolidar os dados. Essas DAGs podem ser iniciadas manualmente ou agendadas automaticamente no Airflow.
    - Incluir o arquivo raw [caduceus.consolidation_anonymized](https://drive.google.com/file/d/1NEzvkoYW7x2eygy5Jdfc-4owXL0wzjOw/view) na pasta `dags/core/raw/`, que não foi incluído por conta do tamanho no github.

## Estrutura das DAGs

Cada DAG no Airflow realiza as seguintes tarefas conforme pedido:

1. **Ingestão de Dados:**: Dag `insert_json_data`, lê os dados brutos do arquivo JSON que esta na `core/raw/`.
2. **Transformação de Dados**: Dag `insert_json_data`, processa e transforma os dados brutos e salva no banco postgres.
3. **Criação do Data Warehouse (DW):**: As demais dags processam os dados do banco postgres para análise, os resultados processados são salvos como arquivos CSV no diretório `core/resultados/`. Segue as Dags e suas correspondentes funções:
    1. **Número total de pacientes por condição (is_present: true) ?**
        - Dag `pat_conditions`
    2. **Número total de pacientes por condição e que são complexos (is_complex:true) ?**
        - Dag `pat_complex_conditions`
    3. **Medicamentos mais prescritos para essas condições ?**
        - Dag `pat_medications`
    4. **Quantos pacientes possuem duas ou mais dessas condições crônicas ao mesmo tempo?**
        - Dag `pat_chronic_conditions`
    5. **Número total de clientes (client_id) ?**
        - Dag `clients_total`
    6. **Número total de pacientes (patient_id) ?**
        - Dag `pat_conditions`

## Parando os Serviços

Para parar e remover os containers, execute:
```bash
docker-compose down
```

## Logs e Monitoramento

Os logs do Airflow são armazenados no diretório `logs/`. Você pode monitorar as execuções e logs detalhados de cada tarefa através da interface do Airflow.

## Autor

Alex Noronha
