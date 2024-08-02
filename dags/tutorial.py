import textwrap
from datetime import datetime, timedelta

# DAG object; DAG 초기화를 위해 사용
from airflow.models.dag import DAG

# Operatorsl; DAG 동작을 위해 사용
from airflow.operators.bash import BashOperator

with DAG(
    "my_first_dag",
    # 이 인자들은 각 operator에 전달됨
    # operator 초기화 중에 각 task별로 오버라이딩이 가능
    default_args={
        "depends_on_past": False,
        "email": ["kimdong799@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # t1, t2 및 t3는 operator를 인스턴스화하여 생성된 task의 예시
    task1 = BashOperator(
        task_id="print_date",
        bash_command="date",  # 현재 날짜와 시간을 문자열로 출력
    )

    task2 = BashOperator(
        task_id="sleep",
        # depends_on_past: False,
        bash_command="sleep 5",  # 5초 동안 대기 상태 유지
        retries=3,
    )

    # Task에 대한 문서를 작성
    task1.doc_md = textwrap.dedent(
        """\
    ### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](https://imgs.xkcd.com/comics/fixing_problems.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # DAG시작 부분에 docstrin이 있는 경우
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # 그렇지 않은 경우 아래와 같이 작성
    templated_command = textwrap.dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
        """
        # """
        # {% for i in range(5) %}
        #     echo "Hello Airflow!"
        # {% endfor %}
        # """
    )

    task3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    task1 >> [task2, task3]
