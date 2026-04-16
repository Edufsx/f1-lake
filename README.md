<!-- omit in toc -->
# F1 Lake

Projeto realizado por Eduardo Ferreira da Silva para predição da probabilidade 
de cada piloto da Fórmula 1 ser campeão da temporada.  

<!-- omit in toc -->
## 📚 Sumário
- [📌 Visão Geral do Projeto](#-visão-geral-do-projeto)
- [🤔 Definição do problema](#-definição-do-problema)
- [🧰 Ferramentas Utilizadas](#-ferramentas-utilizadas)
- [💼 Entendimento do Negócio](#-entendimento-do-negócio)
- [📊 Entendimento dos Dados](#-entendimento-dos-dados)
  - [Análise de Engajamento](#análise-de-engajamento)
  - [Principais Insights](#principais-insights)
- [🛠️ Preparação dos Dados](#️-preparação-dos-dados)
  - [Segmentação de Clientes](#segmentação-de-clientes)
  - [Feature Stores](#feature-stores)
    - [🔄 Ciclo de Vida](#-ciclo-de-vida)
    - [💳 Transacional](#-transacional)
    - [🎓 Plataforma de Cursos](#-plataforma-de-cursos)
  - [Pipeline de Dados](#pipeline-de-dados)
  - [Construção da ABT](#construção-da-abt)
- [👨🏻‍🔬 Modelagem](#-modelagem)
  - [Sample](#sample)
  - [Explore](#explore)
  - [Modify](#modify)
  - [Model](#model)
  - [Assess](#assess)
- [🩺 Avaliação](#-avaliação)
  - [Impacto no Negócio](#impacto-no-negócio)
  - [Limitações](#limitações)
- [🚀 Deploy](#-deploy)
- [🖥️ Como Utilizar](#️-como-utilizar)
- [🔚 Conclusão](#-conclusão)


## 📌 Visão Geral do Projeto
Bem vindo ao meu projeto de predição de fidelidade de clientes utilizando dados do canal Teo Me Why da Twitch. 

Nele o **objetivo** foi construir uma **Tabela Base Analítica** (ABT) e um **modelo** de ***machine learning*** para realizar **predições** sobre a **probabilidade** de um **cliente** se tornar **fiel** nos 28 dias seguintes a uma data específica.

Para construir e orquestrar essas predições, foram utilizados, principalmente, *scripts* **Python**, consultas em **SQL** e conhecimentos de **Estatística** e **Aprendizado de Máquina**.

Para o desenvolvimento do projeto foi utilizada a metodologia *Cross-Industry Standard Process for Data Mining* (CRISP-DM) que estabelece 6 etapas: 
1. **Entendimento do Negócio**;
2. **Entendimento dos Dados**;
3. **Preparação dos Dados**;
4. **Modelagem**;  
5. **Validação**;
6. **Implementação do projeto e acompanhamento**.

Além disso, dentro da etapa de modelagem utilizou-se a metodologia ***Sample-Explore-Modify-Model-Assess*** (SEMMA) desenvolvida pela empresa SAS.

## 🤔 Definição do problema

A principal questão a ser respondida neste projeto é:

`Qual a probabilidade de um cliente se tornar fiel nos próximos 28 dias?`



## 🧰 Ferramentas Utilizadas



## 💼 Entendimento do Negócio


## 📊 Entendimento dos Dados



### Análise de Engajamento



---

### Principais Insights



## 🛠️ Preparação dos Dados





---

### Segmentação de Clientes



---

### Feature Stores

Foram desenvolvidas três *feature stores*, cada uma capturando diferentes dimensões do comportamento:

#### 🔄 Ciclo de Vida


#### 💳 Transacional


#### 🎓 Plataforma de Cursos


---

### Pipeline de Dados


---

### Construção da ABT



## 👨🏻‍🔬 Modelagem


---

### Sample



---

### Explore


### Modify



---

### Model

Foram testados três algoritmos baseados em árvores:

- **Decision Tree**;
- **Random Forest**;
- **AdaBoost**.

O treinamento foi realizado com **Grid Search** + **Validação Cruzada**, utilizando **AUC-ROC** como métrica principal.

Foi construído um pipeline unificando pré-processamento e modelo:

```Python
model_pipeline = pipeline.Pipeline(steps=[
    ('drop_features', drop_features),
    ('imputations', imputers),
    ('encoding', onehot),
    ('model', grid),
])
```

Além disso, utilizou-se o `MLflow` para rastreamento de experimentos, permitindo versionamento de modelos e comparação de métricas.

---

### Assess

Os modelos foram avaliados nas bases:

- **Treino**: ajuste aos dados;
- **Teste**: generalização;
- **OOT**: desempenho em dados futuros.

O resultado obtido na comparação das bases utilizando a AUC-ROC foi o seguinte:

|     Modelo    | Treino | Teste  |  OOT   |
|---------------|--------|--------|--------|
| Decision Tree | 0.8648 | 0.8012 | 0.7846 |
| Random Forest | 0.9302 | 0.8462 | 0.8179 |
|    AdaBoost   | 0.8845 | 0.8531 | 0.8250 |

Escolha do modelo:
- **Random Forest** apresentou melhor desempenho em treino, porém com sinais de *overfitting*;
- **AdaBoost** apresentou maior consistência entre treino, teste e OOT.

👉 O modelo final selecionado foi o `AdaBoost`, por apresentar melhor capacidade de generalização e estabilidade temporal.

📄 O código completo da etapa de modelagem pode ser encontrado em: [src/analytics/train.py](src/analytics/train.py).

## 🩺 Avaliação


### Impacto no Negócio



### Limitações

- O modelo depende de padrões históricos e pode sofrer declínio da capacidade preditiva ao longo do tempo;
- Necessidade de retreinamento periódico para manter a performance;
- Sensibilidade a mudanças no comportamento dos usuários ou no produto.

## 🚀 Deploy



## 🖥️ Como Utilizar



## 🔚 Conclusão