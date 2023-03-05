
from datetime import datetime, timedelta
from textwrap import dedent


from airflow.decorators import dag, task


@dag(
    "Accidents",
    default_args={
        "depends_on_past": False,
        "email": ["laboitatom@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Analyse des accidents en Nouvelle Caldédonie",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["accidents", "nc", "ml"],
)
def accidents_workflow():

    @task.virtualenv(
        task_id="download_data", requirements=["pandas"], system_site_packages=False
    )
    def download_data():
        import pandas as pd

        caracteristiques = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/85cfdc0c-23e4-4674-9bcd-79a970d7269b', delimiter=';')
        caracteristiques =  pd.concat([caracteristiques, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/07a88205-83c1-4123-a993-cba5331e8ae0', delimiter=';')])
        caracteristiques =  pd.concat([caracteristiques, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/e22ba475-45a3-46ac-a0f7-9ca9ed1e283a', delimiter=';')])

        usagers = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/ba5a1956-7e82-41b7-a602-89d7dd484d7a', delimiter=';')
        usagers = pd.concat([usagers, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/78c45763-d170-4d51-a881-e3147802d7ee', delimiter=';')])
        usagers = pd.concat([usagers, pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/36b1b7b3-84b4-4901-9163-59ae8a9e3028', delimiter=';')])
        return pd.merge(caracteristiques, usagers, how='left', left_on=['Num_Acc'], right_on=['Num_Acc'])

    data = download_data()

accidents_workflow()