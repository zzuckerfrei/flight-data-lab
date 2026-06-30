"""OpenSky → GCS (#16, FD-W112)

단일 영역의 state vector 스냅샷을 raw JSON 그대로 GCS(bronze)에 저장한다.
- 인증: OAuth2 client credentials (시크릿은 k8s Secret → env)
- 적재: raw 불변 보존(Bronze). 덮어쓰지 않고 스냅샷 시각으로 파티셔닝 경로에 추가.
- MVP: 단일 영역 + 수동 트리거. 다영역·스케줄·멱등성은 W2.
"""
import json
import os

import pendulum
import requests
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"
BUCKET = "flight-data-lab-501011-bronze"

# 단일 테스트 영역 (서유럽 비교군). 다영역은 W2에서 config화.
REGION = "west_europe"
BBOX = {"lamin": 45, "lamax": 52, "lomin": 2, "lomax": 12}


def fetch_and_store():
    # 1) OAuth2 토큰 발급 (client credentials). 시크릿은 env에서만 읽는다.
    token_res = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": os.environ["OPENSKY_CLIENT_ID"],
            "client_secret": os.environ["OPENSKY_CLIENT_SECRET"],
        },
        timeout=30,
    )
    token_res.raise_for_status()  # 실패(4xx/5xx)면 예외 → task 실패 → Airflow 재시도
    access_token = token_res.json()["access_token"]

    # 2) /states/all 호출 (bbox로 영역 한정)
    states_res = requests.get(
        STATES_URL,
        params=BBOX,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
    states_res.raise_for_status()
    payload = states_res.json()
    snapshot_time = payload["time"]  # unix epoch (스냅샷 시각)

    # 3) GCS 업로드 (raw JSON 그대로 = Bronze, 불변)
    dt = pendulum.from_timestamp(snapshot_time, tz="UTC").format("YYYYMMDD")
    path = f"opensky/raw/region={REGION}/dt={dt}/states_{snapshot_time}.json"
    client = storage.Client()  # GOOGLE_APPLICATION_CREDENTIALS(ADC)로 자동 인증
    blob = client.bucket(BUCKET).blob(path)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")

    aircraft = len(payload.get("states") or [])
    print(f"saved gs://{BUCKET}/{path} (aircraft={aircraft})")


with DAG(
    dag_id="opensky_to_gcs",
    schedule=None,  # 수동 트리거 (MVP)
    start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
    catchup=False,
    tags=["opensky", "bronze"],
) as dag:
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store",
        python_callable=fetch_and_store,
    )
