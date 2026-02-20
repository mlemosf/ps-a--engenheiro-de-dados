# Processo seletivo engenheiro de dados - +A Educacao

Esse repositório diz respeito ao processo seletivo da +A Educação para engenheiro de dados.

## Etapas

Para a realização desse projeto, segui as seguintes etapas:

1. Entendimento dos dados: Conhecer os dados de origem que devem ser consumidos;
2. Identificar informações de negócio: Pensar em quais insigths de negócio devem ser atingidos após a construção de toda a automação;
3. Modelagem de esquema: Para conseguir extrair as informações de negócio, quais entidades e relacionamentos devem existir;
4. Carga: Após a modelagem de esquema, como extrair os dados da origem e carregá-los nas entidades de destino;
5. Análise: Construção de buscas SQL para extrair insights dos dados

## Entendimento dos dados
  
Os dados utilizados foram da [Fake store API](https://fakestoreapi.com/), que fornece de dados simulando verdadeiras plataformas de e-comerce.
Analisando a plataforma, vi que existem 3 entidades de negócio principais:

- Produtos;
- Usuários;
- Carrinhos;

Produtos são associados a usuários quando os mesmos os adicionam nos carrinhos. Cada usuário pode ter 1 a N instâncias de um produto em seu carrinho. Ca

## Insigths de negócio

Após analisar os dados gerados, podemos identificar algumas perguntas relevantes para o stakeholder:

- Quais produtos são mais populares (quais produtos existem em mais carrinhos)?
- Quais categorias de produto fazem mais sucesso?

Além disso, algumas perguntas devem ser feitas sobre os dados em si:

- Com que frequência as dimensões mudam?

## Modelagem de dados

Após identificar perguntas de interesse para o negócio, é necessário realizar a modelagem de dados para responder essas perguntas.
Uma arquitetura moderna para garantir linhagem e qualidade de dados é a **arquitetura medalhão**, onde temos 3 níveis de qualidade de dados:

- **Bronze**: Dados em formato mais próximo possível do original, organizado em tabelas;  
- **Prata**: Dados próximos ao original, mas enriquecidos com informações como data de ingestão e arquivo de origem (se houver). Nessa etapa é importante tratar da verificação de integridade referencial;  
- **Ouro**: Aqui temos as tabelas fato onde serão realizadas análises, realizando agregações sem a necessidade de joins complexos.  

![diagrama](diagrama-modelagem.png)

Essa arquitetura garante linhagem de dados, onde podemos ver como os dados se transformam ao longo do processo de carregamento no Data Warehouse.
Abaixo segue a imagem do diagrama que documenta as tabelas das camadas bronze e prata (descritas com os prefixos bronze_ e silver_) e as tabelas outro (descritas com os prefixos dim_ e fact_).
As tabelas bronze e prata representam modelos de dados próximos ao original e com algum tipo de tratamento. Já as tabelas ouro representam o data warehouse, seguindo um modelo estrela do modelo dimensional de Kimball.

O data warehouse é composto de 1 tabela fato e 4 tabelas de dimensão:

- fact_products_in_cart: Tabela fato que agrega os produtos no carinho em 4 diferentes dimensões: usuário, carrinho, geolocalização e produto;  
- dim_user: Dimensão de usuários, seguindo modelo SCD tipo 1;  
- dim_product: Dimensão de produtos, seguindo modelo SCD tipo 1;  
- dim_cart: Dimensão de carrinhos, seguindo modelo SCD tipo 1;  
- dim_geolocation: Dimensão de geolocalização de usuários, seguindo modelo SCD tipo 1.

## Carga
