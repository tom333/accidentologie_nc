from prefect import flow, task, get_run_logger
import pandas as pd
from prefect.filesystems import GitHub

github_block = GitHub.load("github-accidentologie")


@task
def get_data():
    
    caracteristiques = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/85cfdc0c-23e4-4674-9bcd-79a970d7269b', delimiter=';')
    caracteristiques =  pd.concat([caracteristiques, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/07a88205-83c1-4123-a993-cba5331e8ae0', delimiter=';')])
    caracteristiques =  pd.concat([caracteristiques, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/e22ba475-45a3-46ac-a0f7-9ca9ed1e283a', delimiter=';')])

    usagers = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/ba5a1956-7e82-41b7-a602-89d7dd484d7a', delimiter=';')
    usagers = pd.concat([usagers, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/78c45763-d170-4d51-a881-e3147802d7ee', delimiter=';')])
    usagers = pd.concat([usagers, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/36b1b7b3-84b4-4901-9163-59ae8a9e3028', delimiter=';')])
    return pd.merge(caracteristiques, usagers, how='left', left_on=['Num_Acc'], right_on=['Num_Acc'])


@task
def filter_nc(df_all):
    df = df_all.loc[df_all.dep.isin(['988']), ['jour' , 'mois', 'an' , 'grav', 'hrmn', 'lat', 'long', 'lum', 'atm']]

    df['lum'] = df['lum'].astype("category")
    df['atm'] = df['atm'].astype("category")
    df['jour'] = df['jour'].astype("category")
    df['mois'] = df['mois'].astype("category")
    df['an'] = df['an'].astype("category")
    df['grav'] = df['grav'].astype("category")

    df = df.applymap(lambda x: x.strip().replace(',', '.') if isinstance(x, str) else x)
    df["lat"] = pd.to_numeric(df["lat"], downcast="float")
    df["long"] = pd.to_numeric(df["long"], downcast="float")
    return df


@task 
def train(df):
    df['day_of_year'] = pd.to_datetime(df['jour'].astype(str) + "/" + df['mois'].astype(str) + "/" + df['an'].astype(str), format='%d/%m/%Y').dt.day_of_year
    df['date'] = pd.to_datetime(df['jour'].astype(str) + "/" + df['mois'].astype(str) + "/" + df['an'].astype(str), format='%d/%m/%Y')
    data = df.groupby(['date']).size().reset_index(name="y")
    df.set_index("date", inplace=True)
    df = df.resample("1D").mean()
    df["time_idx"] = (df.index.view(int) / pd.Timedelta("1D").value).astype(int)
    df["time_idx"] -= df["time_idx"].min()

    df["constant"] = 0
    get_run_logger(data.head())



@flow(name="Accidentologie en NC")
def accidentologie():
    df = get_data()
    df = filter_nc(df)
    train(df)

if __name__ == "__main__":
    df = accidentologie()
    get_run_logger(df.head())
