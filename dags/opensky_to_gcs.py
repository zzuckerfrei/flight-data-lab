"""OpenSky → GCS (#18, FD-W202)

4개 영역의 state vector 스냅샷을 10분 주기로 raw JSON 그대로 GCS(bronze)에 저장한다.
- 다영역: Dynamic Task Mapping(.expand)으로 영역 수만큼 task 자동 생성(영역별 독립 성공/실패).
- 스케줄: 10분 간격, catchup=False (과거분 소급 안 함, 현재 스냅샷만).
- 인증: OAuth2 client credentials (시크릿은 k8s Secret → env). 매 실행 새 토큰이라 30분 만료 무관.
- 적재: raw 불변 보존(Bronze). 스냅샷 시각으로 파티셔닝, 같은 스냅샷=같은 경로.
"""
import json
import os

import pendulum
import requests
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack_alerts import notify_failure  # 실패 시 Slack 알림 (#33)

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"
BUCKET = "flight-data-lab-501011-bronze"

# 수집 영역 4곳 (docs/collection-regions.md). 각 원소가 op_kwargs로 fetch_and_store(region, bbox)에 매핑됨.
# 분쟁2(우크라·중동) + 비교2(서유럽·한국). 호출당 합계 10크레딧 × 144회/일 = 1,440/일(4000의 36%).
REGIONS = [
    {"region": "ukraine",     "bbox": {"lamin": 44, "lamax": 53, "lomin": 22,  "lomax": 40}},
    {"region": "middle_east", "bbox": {"lamin": 25, "lamax": 40, "lomin": 38,  "lomax": 63}},
    {"region": "west_europe", "bbox": {"lamin": 45, "lamax": 52, "lomin": 2,   "lomax": 12}},
    {"region": "korea",       "bbox": {"lamin": 33, "lamax": 39, "lomin": 124, "lomax": 132}},
]


def fetch_and_store(region, bbox):
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
        params=bbox,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
    states_res.raise_for_status()
    payload = states_res.json()
    snapshot_time = payload["time"]  # unix epoch (스냅샷 시각)

    # 3) GCS 업로드 (raw JSON 그대로 = Bronze, 불변)
    dt = pendulum.from_timestamp(snapshot_time, tz="UTC").format("YYYYMMDD")
    path = f"opensky/raw/region={region}/dt={dt}/states_{snapshot_time}.json"
    client = storage.Client()  # GOOGLE_APPLICATION_CREDENTIALS(ADC)로 자동 인증
    blob = client.bucket(BUCKET).blob(path)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")

    aircraft = len(payload.get("states") or [])
    print(f"saved gs://{BUCKET}/{path} (region={region} aircraft={aircraft})")


with DAG(
    dag_id="opensky_to_gcs",
    schedule="*/10 * * * *",  # 10분 간격
    start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
    catchup=False,  # 시작일~현재 사이 밀린 실행분 소급 안 함
    tags=["opensky", "bronze"],
    # on_failure_callback을 default_args로 → 모든 task(매핑된 영역별 포함)에 적용(task-level).
    #   어느 task 실패인지 알아야 게이트/일반 실패를 구분할 수 있어 DAG-level 아닌 task-level로 둔다.
    default_args={"on_failure_callback": notify_failure},
) as dag:
    # Dynamic Task Mapping: REGIONS 길이만큼 task 자동 생성. 각 dict가 op_kwargs로 전달됨.
    fetch_and_store_task = PythonOperator.partial(
        task_id="fetch_and_store",
        python_callable=fetch_and_store,
    ).expand(op_kwargs=REGIONS)
