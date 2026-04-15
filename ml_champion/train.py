# %%
import pandas as pd
from feature_engine import imputation
from sklearn import ensemble, metrics, pipeline, model_selection
import mlflow
import matplotlib.pyplot as plt
import dotenv
import os
#%%
# Opções de visualização dos objetos dos pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

# Carrega as variáveis contidas no arquivo .env
dotenv.load_dotenv()

# Configura endereço do servidor e experimento do MLflow 
mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
mlflow.set_experiment(experiment_id=1)
#%%
# Carrega a Analytical Base Table (ABT) para treinamento do modelo 
df = pd.read_csv("../data/abt_f1_drivers_champion.csv", sep=";")

df["dt_ref"] = pd.to_datetime(df["dt_ref"])
df["year"] = df["dt_ref"].dt.year

#%%
# --- SAMPLE: Treino/Teste e Out Of Time(OOT) ---

# Base Out Of Time(OOT) para validação temporal do modelo
df_oot = df[df["year"] == 2025].copy()

# Bases de Treino/Teste separada da OOT
df_analytics = df[df["year"] < 2025].copy()

#%%
# --- SAMPLE: Retira as 5 últimas corridas da base de Treino/Teste ---
# Separa as datas de referência (corridas) e anos distintos
df_year_round = df_analytics[["year", "dt_ref"]].drop_duplicates()

# Organiza as corridas de cada ano em ordem decrescente e as enumera
df_year_round = (df_year_round.sort_values(["year", "dt_ref"], 
                 ascending=[False, False])) 
df_year_round["row_number"] = (df_year_round.groupby("year").cumcount())

# Retira as 5 últimas corridas de cada ano para evitar data leakage
df_year_round = df_year_round[df_year_round["row_number"] > 5]

df_year_round = df_year_round.drop("row_number", axis=1)
#%%
# --- SAMPLE: Divide Treino e Teste---
# Define as diferentes temporadas que um piloto competiu (ordem decrescente) 
df_driver_year = df_analytics[["driverid", "year", "flChampion"]].drop_duplicates()
df_driver_year.sort_values(["driverid", "year"], ascending=[True, False])

# Divide as bases de Treino (80%) e Teste (20%)  
train, test = model_selection.train_test_split(
    df_driver_year,
    random_state=42,
    train_size=0.8,
    stratify = df_driver_year["flChampion"] # Estratifica a variável target
)

print("Taxa de Campeões Treino:", train["flChampion"].mean())
print("Taxa de Campeões Teste:", test["flChampion"].mean())

# Recupera as informações das temporadas selecionadas nas duas bases
df_train = train.merge(df_analytics).merge(df_year_round, how='inner')
df_test = test.merge(df_analytics).merge(df_year_round, how='inner')
#%%
# --- SAMPLE: features e target das bases
# Define as features para treinamento do modelo 
features = df_train.columns[5:-3]

# Separa as matrizes das features e o vetor do target em cada base
X_train, y_train = df_train[features], df_train['flChampion']
X_test, y_test = df_test[features], df_test['flChampion']
X_oot, y_oot = df_oot[features], df_oot['flChampion']

# %%
# --- EXPLORE: Missing Values ---
# Verifica quais colunas apresentam valores faltantes e quantidade deles
isna = X_train.isna().sum()
isna[isna>0]
#%%
# --- MODIFY: Imputação Missing ---
# Define a imputação de -1000 nos valores faltantes
# para que o modelo aprenda padrões associados à ausência de dados
missing = imputation.ArbitraryNumberImputer(
    -1000,
    variables=X_train.columns.tolist())

# %%
# --- MODEL: Definição de modelos ---

# Dicionário com modelos a serem testados e seus hiperparâmetros fixos
models = {
    "ada_boost": ensemble.AdaBoostClassifier(
        n_estimators=500,
        learning_rate=0.001,
        random_state=42
        ),
    "random_forest": ensemble.RandomForestClassifier(
        min_samples_leaf=50,
        n_estimators=500,
        random_state=42,
        n_jobs=2
        ),     
} 
model_names = list(models.keys())    

#%%
# Loop para testes dos modelos
for model_name in model_names:

    # Seleciona modelo para treinamento
    clf = models[model_name] 

    # Executa experimento no MLflow
    with mlflow.start_run(run_name=model_name):

        # Pipeline com imputação e Modelo        
        model = pipeline.Pipeline(steps=[
        ('Imputation', missing),
        (model_name, clf)
        ])
        
        # Loga hiperparâmetros do modelo e estratégias de Imputação no MLflow
        mlflow.log_params(clf.get_params())
        mlflow.log_param("imputation_strategy", "arbitrary")
        mlflow.log_param("imputation_value", -1000)

        # --- MODEL ---
        # Treinamento do Pipeline
        model.fit(X_train, y_train)
        
        # --- ASSESS: Train ---

        print(f"Métricas utilizando o {model_name}:")
        
        # Predição da probabilidade de ser campeão na base de Treino
        y_train_proba = model.predict_proba(X_train)[:,1]
        
        # AUC Score na base Treino
        auc_train = metrics.roc_auc_score(y_train, y_train_proba)
        print("AUC Train:", auc_train)

        # --- ASSESS: Teste ---

        # Predição da probabilidade de ser campeão na base de Teste
        y_test_proba = model.predict_proba(X_test)[:,1]

        # AUC Score na base de Teste
        auc_test = metrics.roc_auc_score(y_test, y_test_proba)
        print("AUC Test:", auc_test)

        # --- ASSESS: Out Of Time (OOT) ---
        
        # Predição da probabilidade de ser campeão na base OOT
        y_oot_proba = model.predict_proba(X_oot)[:,1]

        # AUC Score na base OOT
        auc_oot = metrics.roc_auc_score(y_oot, y_oot_proba)
        print("AUC OOT:", auc_oot)

        # Log manual de métricas no MLflow
        mlflow.log_metrics({"ROC Train" : auc_train,
                        "ROC Test" : auc_test,
                        "ROC OOT" : auc_oot})

        # --- Curva ROC ---

        # Calcula curvas ROC nas bases de treino, teste e OOT
        roc_train = metrics.roc_curve(y_train, y_train_proba)
        roc_test = metrics.roc_curve(y_test, y_test_proba)
        roc_oot = metrics.roc_curve(y_oot, y_oot_proba)

        # Plot das Curvas
        plt.figure(dpi=100)
        plt.plot(roc_train[0], roc_train[1])
        plt.plot(roc_test[0], roc_test[1])
        plt.plot(roc_oot[0], roc_oot[1])
        plt.legend([f"Treino: {auc_train:.4f}", 
                f"Teste: {auc_test:.4f}",
                f"OOT: {auc_oot:.4f}"])
        plt.grid(True)
        plt.title(f"Curva ROC ({model_name})")

        # Salva e registra a Curva ROC como artefato no MLflow
        plt.savefig(f"artifacts/roc_curve_{model_name}.png")
        mlflow.log_artifact(f"artifacts/roc_curve_{model_name}.png")

        # --- Feature Importance ---
        
        # Extrai o modelo do pipeline treinado
        rf = model.named_steps[model_name]
        
        # Calcula a importância das features com base no modelo treinado
        feature_importance = pd.Series(
            rf.feature_importances_,
            index=X_train.columns
        ).sort_values(ascending=False)

        # Salva a feature importance das variáveis em um arquivo markdown
        feature_importance.to_markdown(f"artifacts/feature_importance_{model_name}.md")
        
        # Registra o arquivo como artefato no MLflow para versionamento
        mlflow.log_artifact(f"artifacts/feature_importance_{model_name}.md")

        # --- LOG Final ---
        # Re-treina o modelo final com todo o dataset disponível
        model.fit(df[features], df["flChampion"])

        # Salva o modelo treinado no MLflow
        mlflow.sklearn.log_model(model, name="model")
        
# %%
# Cria um objeto contendo o modelo treinado e a lista de features utilizadas
model_df = pd.Series({
    "model": model,
    "features": features,
})

# Salva o objeto serializado em formato .pkl para uso em Aplicação Web
model_df.to_pickle("../app_for_streamlit_cloud/model.pkl")