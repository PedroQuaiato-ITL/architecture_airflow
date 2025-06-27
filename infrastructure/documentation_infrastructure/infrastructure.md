# Bootstrap de Instalação do Apache Airflow

Este diretório contém scripts automatizados de instalação para configurar o Apache Airflow (versão 2.7.3) e bibliotecas adicionais como pandas e requests.

A instalação está separada por sistema operacional:

- Linux/macOS → install_airflow.sh
- Windows (CMD) → install_airflow.bat
- Windows PowerShell → install_airflow.ps1

## Estrutura da pasta

infrastructure/
└── bootstrap/
    ├── install_airflow.sh
    ├── install_airflow.bat
    ├── install_airflow.ps1
    └── README.md

## Pré-requisitos

- Python entre as versões 3.8 e 3.11
- pip instalado corretamente
- Acesso à internet
- Recomendado: uso de ambiente virtual (venv ou conda)

## Como usar os scripts

### Linux ou macOS

Acesse a pasta pelo terminal:

cd infrastructure/bootstrap

Dê permissão de execução:

chmod +x install_airflow.sh

Execute o script:

./install_airflow.sh

Recomendado: ativar ambiente virtual

python3 -m venv venv  
source venv/bin/activate

### Windows (CMD)

Abra o Prompt de Comando, acesse a pasta:

cd infrastructure\bootstrap

Execute o script:

install_airflow.bat

Ou dê duplo clique no arquivo.

### Windows PowerShell

Libere a execução de scripts (caso necessário):

Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

Acesse a pasta:

cd infrastructure/bootstrap

Execute o script:

.\install_airflow.ps1

## O que os scripts fazem

1. Instalam o Apache Airflow 2.7.3 com os constraints oficiais do projeto:

pip install "apache-airflow==2.7.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"

2. Instalam bibliotecas adicionais comumente utilizadas:

pip install requests pandas

## Possíveis erros e soluções

| Mensagem de erro                                         | Possível causa                              | Como resolver                                                                 |
|----------------------------------------------------------|---------------------------------------------|-------------------------------------------------------------------------------|
| pip: command not found                                   | Python ou pip não instalados corretamente   | Instalar Python e adicionar ao PATH                                           |
| Permission denied                                        | Script .sh sem permissão de execução        | Executar chmod +x install_airflow.sh                                         |
| Execution of scripts is disabled on this system          | PowerShell bloqueando execução de scripts   | Executar Set-ExecutionPolicy RemoteSigned -Scope CurrentUser                  |
| Could not find a version that satisfies the requirement  | Python fora do intervalo aceito pelo Airflow| Usar Python entre 3.8 e 3.11 ou ajustar ambiente virtual                      |

## Recomendação de ambiente virtual

python -m venv venv  
source venv/bin/activate     (Linux/macOS)  
venv\Scripts\activate        (Windows)

## Expansão futura

Este diretório pode ser expandido para incluir:

- Arquivos docker-compose
- Scripts de setup de banco de dados
- Scripts de criação de conexões e variáveis no Airflow
- Instalação de plugins e DAGs automaticamente
