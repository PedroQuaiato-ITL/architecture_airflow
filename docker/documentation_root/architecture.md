Arquitetura do Airflow
Essa arquitetura prioriza funcionalidade, modularidade e, principalmente, simplicidade. O objetivo é facilitar para o time de dados a inserção de novas funcionalidades e pipelines dentro do sistema Airflow.

Base Tecnológica
Utilizamos o Docker como motor principal para orquestrar o ambiente. Ele é responsável por subir o serviço do Airflow e seus componentes essenciais, como:

webserver (UI)

scheduler

init (para inicialização e configuração)

Como subir o projeto
Acesse a pasta docker na raiz do projeto.

Dentro dela, você encontrará o arquivo docker-compose.yaml, que contém todas as instruções necessárias para subir o ambiente do Airflow.

Ainda na pasta docker, temos algumas subpastas obrigatórias para o funcionamento correto do Airflow:

dags: onde ficam as definições das DAGs.

plugins: para customizações e operadores próprios.

logs: onde são salvos os logs de execução das DAGs.

Padrão de Organização das DAGs
Para manter a padronização e facilitar o entendimento entre os membros do time, seguimos uma estrutura de nomeação consistente para os arquivos de DAG:

DAG_[processo]_[tipo de processo]_[foco]

Exemplo:

DAG_atualizacao_desempenho_revenda

atualizacao: processo principal
desempenho: tipo de processo
revenda: foco/entidade

Essa organização ajuda a manter o repositório limpo e fácil de navegar, além de facilitar a identificação do propósito de cada DAG.

Dentro da pastas de DAGS possuimos tambem a pasta MODULES, nela contemos modulos que podem ser reutilizados por mais processos,são processos mais genericos.