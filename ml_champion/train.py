# %%
import pandas as pd
from feature_engine import imputation
from sklearn import ensemble, metrics, pipeline, model_selection
import mlflow
import matplotlib.pyplot as plt
import dotenv
import os

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

dotenv.load_dotenv()

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))

mlflow.set_experiment(experiment_id = 1)
#%%
df = pd.read_csv("../data/abt_f1_drivers_champion.csv", sep=";")
#%%
# --- SAMPLING --- 
df["dt_ref"] = pd.to_datetime(df["dt_ref"])
df["year"] = df["dt_ref"].dt.year
#%%
df_oot = df[df["year"] == 2025].copy()

df_analytics = df[df["year"] < 2025].copy()

#%%
df_year_round = df_analytics[["year", "dt_ref"]].drop_duplicates()

df_year_round = (df_year_round.sort_values(["year", "dt_ref"], 
                 ascending=[False, False]))
#%%
df_year_round["row_number"] = (df_year_round
                               .groupby("year")
                               .cumcount())


df_year_round = df_year_round[df_year_round["row_number"] > 5]
df_year_round = df_year_round.drop("row_number", axis=1)
#%%
df_driver_year = df_analytics[["driverid", "year", "flChampion"]].drop_duplicates()
df_driver_year.sort_values(["driverid", "year"], ascending=[True, False])

train, test = model_selection.train_test_split(
    df_driver_year,
    random_state=42,
    train_size=0.8,
    stratify = df_driver_year["flChampion"]
)

print("Taxa de Campeões Treino:", train["flChampion"].mean())
print("Taxa de Campeões Treino:", test["flChampion"].mean())

df_train = train.merge(df_analytics).merge(df_year_round, how='inner')
df_test = test.merge(df_analytics).merge(df_year_round, how='inner')
#%%
features = df_train.columns[5:-3]

#%%
X_train, y_train = df_train[features], df_train['flChampion']
X_test, y_test = df_test[features], df_test['flChampion']
X_oot, y_oot = df_oot[features], df_oot['flChampion']

# %%
# --- Explore ---
isna = df_train.isna().sum()
isna[isna>0]

#%%
df[df["avg_overtake_race_last_10"].isna()]

#%%
missing = imputation.ArbitraryNumberImputer(
    -1000,
    variables=X_train.columns.tolist())
    

# %%
 
clf = ensemble.RandomForestClassifier(
    min_samples_leaf=50,
    n_estimators=500,
    random_state=42,
    n_jobs=2)

""" clf = ensemble.AdaBoostClassifier(
    n_estimators=500,
    learning_rate=0.001,
    random_state=42)
 """

model = pipeline.Pipeline(steps=[
    ('Imputation', missing),
    ('RandomForest', clf)
])
#%%
with mlflow.start_run():

    model.fit(X_train, y_train)

    y_train_proba = model.predict_proba(X_train)[:,1]
    roc_train = metrics.roc_curve(y_train, y_train_proba)
    auc_train = metrics.roc_auc_score(y_train, y_train_proba)
    print("AUC Train:", auc_train)

    y_test_proba = model.predict_proba(X_test)[:,1]
    auc_test = metrics.roc_auc_score(y_test, y_test_proba)
    roc_test = metrics.roc_curve(y_test, y_test_proba)
    print("AUC Test:", auc_test)

    y_oot_proba = model.predict_proba(X_oot)[:,1]
    auc_oot = metrics.roc_auc_score(y_oot, y_oot_proba)
    roc_oot = metrics.roc_curve(y_oot, y_oot_proba)
    print("AUC OOT:", auc_oot)

    mlflow.log_metrics({"ROC Train" : auc_train,
                       "ROC Test" : auc_test,
                       "ROC OOT" : auc_oot})

    plt.figure(dpi=100)
    plt.plot(roc_train[0], roc_train[1])
    plt.plot(roc_test[0], roc_test[1])
    plt.plot(roc_oot[0], roc_oot[1])
    plt.legend([f"Treino: {auc_train:.4f}", 
            f"Teste: {auc_test:.4f}",
            f"OOT: {auc_oot:.4f}"])
    plt.grid(True)
    plt.title("Curva ROC")
    plt.savefig("roc_curve.png")
    mlflow.log_artifact("roc_curve.png")

    rf = model.named_steps["RandomForest"]
    feature_importance = pd.Series(
        rf.feature_importances_,
        index=X_train.columns
    ).sort_values(ascending=False)
    feature_importance.to_markdown("feature_importance.md")
    mlflow.log_artifact("feature_importance.md")

    model.fit(df[features], df["flChampion"])
    mlflow.sklearn.log_model(model, name="model")

# %%
model_df = pd.Series({
    "model": model,
    "features": features,
})

model_df.to_pickle("../app_web/model.pkl")