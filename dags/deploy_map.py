"""Kepler 지도 자동배포 DAG (#45).

dbt_transform이 marts를 갱신한 뒤, kepler-map 전용 이미지 Pod를 띄워
BQ marts → keplergl HTML → GCS index.html 배포를 자동 실행한다(준실시간 공개 지도).

구조(dbt와 동일한 K8s 2겹): worker Pod가 이 task를 받아 → KubernetesPodOperator로
  kepler-map:1.0.0 Pod를 따로 생성 → build_and_deploy.py 실행 → 끝나면 Pod 정리.

인증: SA키(gcp-sa-key Secret)를 볼륨으로 주입 → GOOGLE_APPLICATION_CREDENTIALS로 BQ read + GCS write.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

from utils.slack_alerts import notify_failure  # 실패 시 Slack 알림 (#33)

# SA키를 Pod의 /root/gcp/key.json으로 주입(dbt와 동일 패턴). gcp-sa-key Secret의 실제 키명=key.json.
gcp_key = Secret(
    deploy_type="volume",
    deploy_target="/root/gcp",
    secret="gcp-sa-key",
    key="key.json",
)

with DAG(
    dag_id="deploy_map",
    # dbt_transform(UTC 01:00) 이후 02:00 → marts 갱신본으로 지도 배포. 준실시간(하루 지연).
    schedule="0 2 * * *",
    start_date=datetime(2026, 7, 22),
    catchup=False,
    tags=["kepler", "deploy", "map"],
    default_args={"on_failure_callback": notify_failure},
) as dag:
    KubernetesPodOperator(
        task_id="build_and_deploy",
        name="kepler-map-deploy",
        namespace="airflow",
        image="kepler-map:1.0.0",
        image_pull_policy="IfNotPresent",   # kind load한 로컬 이미지 → 원격 pull 금지
        cmds=["python", "/app/build_and_deploy.py"],
        secrets=[gcp_key],
        env_vars={
            "GOOGLE_APPLICATION_CREDENTIALS": "/root/gcp/key.json",
            "MAP_DAYS": "3",                # 최근 3일 관심지역
        },
        # 성공 Pod는 삭제(누적 방지), 실패 Pod만 보존(부검용).
        # 애플리케이션 로그는 #43 GCS remote logging에 남으므로 성공 Pod를 남길 이유가 없다.
        # 단 Pod가 아예 못 뜨는 실패(이미지 pull 실패·OOMKilled·스케줄 불가)는 stdout이 없어
        # GCS 로그에도 안 남는다 → 그 경우만 kubectl describe가 필요하므로 실패 Pod는 남긴다.
        on_finish_action="delete_succeeded_pod",
        get_logs=True,
    )
