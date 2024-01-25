from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain


from datetime import datetime, timedelta

default_args = {
    'retry' : 5,
    'retry_interval' : timedelta(minutes=3)
}

#def _baixando_dados(**kwargs):
#    with open('/tmp/meu_arquivo.txt', 'w') as f:
#        f.write('meus dados')

def _baixando_dados(ti, **kwargs):
    with open('/tmp/meu_arquivo.txt', 'w') as f:
        f.write('meus dados')
    ti.xcom_push(key='key_anselmo', value=43)

def _usando_xcom(ti):
    my_xcom = ti.xcom_pull(key='key_anselmo', task_ids=['baixando_dados'])
    print(my_xcom)


#Instanciando um objeto DAG
with DAG (dag_id='simple_dag', 
          schedule_interval=timedelta(minutes=30),
          default_args=default_args,
          start_date=datetime(2023,1,20),
          catchup=False) as dag:

        baixando_dados = PythonOperator(
            task_id = 'baixando_dados',
            python_callable = _baixando_dados
        )

        esperando_dados = PythonOperator(
            task_id='esperando_dados',
            python_callable=_usando_xcom
        )

        #esperando_dados = FileSensor(
        #    task_id = 'esperando_dados',
        #    fs_conn_id = 'dados_do_temp',   #Crio essa conn em connections no UI
        #    filepath = 'meu_arquivo.txt'
        #)

        processando_dados = BashOperator(
            task_id = 'processando_dados',
            bash_command = 'exit 0'         #vai simular um erro na Task
        )

        ## Metodo 1: de dependencias (por escrito)
        #esperando_dados.set_downstream(processando_dados)

        ##Metodo 2: usando bitshift
        baixando_dados >> esperando_dados >> processando_dados
        #baixando_dados >> [esperando_dados, processando_dados]

        ##Metodo 3: Usando chain (depende de importacao do models baseoperator)
        #chain(baixando_dados, esperando_dados, processando_dados)