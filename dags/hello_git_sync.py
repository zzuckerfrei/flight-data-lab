"""git-sync 동작 검증용 테스트 DAG. (#12 검증 후 삭제 예정)"""
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("git-sync 동작 확인! DAG가 클러스터에 동기화됐다.")


with DAG(
    dag_id="hello_git_sync",
    schedule=None,                                        # 자동 스케줄 없음 → 수동 트리거만
    start_date=pendulum.datetime(2026, 6, 29, tz="UTC"),  # DAG 유효 시작 시점
    catchup=False,                                        # 과거 구간 소급 실행 안 함
    tags=["test"],                                        # UI 필터용 태그
) as dag:
    hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,   # 이 task가 실행할 함수
    )
