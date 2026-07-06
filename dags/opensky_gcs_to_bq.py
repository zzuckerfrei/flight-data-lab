"""OpenSky GCS → BigQuery bronze 적재 (#17, FD-W201)

GCS raw JSON 스냅샷을 "봉투"에 담아 BigQuery bronze 테이블에 적재한다.
- B안(메달리온): raw 통짜 JSON 보존(파싱은 dbt staging). 1행 = 1스냅샷.
- 봉투 = bronze 4컬럼 구조: snapshot_time(TIMESTAMP) / region / raw(JSON) / _loaded_at.
  * raw는 dict 그대로 넣음(json.dumps 이중직렬화 금지 — JSON 경로 접근 불가해짐).
- MVP: 수동 트리거 + 특정 날짜(dt) 범위. 멱등성(#19)·스케줄·의존성(#28)은 이후 이슈.
  * dt는 트리거 conf로 지정(기본=오늘 UTC). 예: {"dt": "20260706"}
"""
import json
from datetime import datetime, timezone

import pendulum
from google.cloud import storage, bigquery

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = "flight-data-lab-501011-bronze"
TABLE = "flight-data-lab-501011.flight_data.opensky_states_bronze"
REGIONS = ["ukraine", "middle_east", "west_europe", "korea"]


def load_gcs_to_bq(**context):
    # 적재 대상 날짜: 트리거 conf.dt 우선, 없으면 오늘(UTC)
    dt = (context["dag_run"].conf or {}).get("dt") or pendulum.now("UTC").format("YYYYMMDD")

    gcs = storage.Client()  # SA키(ADC)로 인증 — opensky_to_gcs와 동일
    rows = []
    for region in REGIONS:
        prefix = f"opensky/raw/region={region}/dt={dt}/"
        for blob in gcs.list_blobs(BUCKET, prefix=prefix):
            if not blob.name.endswith(".json"):
                continue
            raw = json.loads(blob.download_as_text())  # OpenSky 원본 {time, states}
            # 봉투 담기(검증된 구조): 원본은 raw에 dict 그대로, 겉에 메타
            rows.append({
                "snapshot_time": datetime.fromtimestamp(raw["time"], tz=timezone.utc).isoformat(),
                "region": region,
                "raw": raw,
                "_loaded_at": datetime.now(timezone.utc).isoformat(),
            })

    if not rows:
        print(f"적재 대상 없음 (dt={dt})")
        return

    # 멱등 적재(#19): dt 파티션을 통째로 교체(WRITE_TRUNCATE) → 재실행해도 행 수 불변.
    #  - destination에 $dt(파티션 데코레이터) → "그 날짜 파티션만" 대상(다른 날짜 무영향)
    #  - WRITE_TRUNCATE → 기존 삭제+새 적재를 원자적으로(중간 상태 없음)
    bq = bigquery.Client()
    job = bq.load_table_from_json(
        rows, f"{TABLE}${dt}",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            # 스키마 명시 필수: WRITE_TRUNCATE는 테이블을 재정의(autodetect)하는데,
            # raw(dict)를 RECORD로 추론해 테이블의 JSON 타입과 충돌(400). 명시로 JSON 강제.
            schema=[
                bigquery.SchemaField("snapshot_time", "TIMESTAMP"),
                bigquery.SchemaField("region", "STRING"),
                bigquery.SchemaField("raw", "JSON"),
                bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
            ],
        ),
    )
    job.result()  # 완료 대기(실패면 예외 → task 실패 → Airflow 재시도)
    print(f"적재 완료: dt={dt}, {job.output_rows}행 ({len(REGIONS)}영역)")


with DAG(
    dag_id="opensky_gcs_to_bq",
    schedule=None,  # 수동 트리거 (MVP). 스케줄·의존성은 #28
    start_date=pendulum.datetime(2026, 7, 6, tz="UTC"),
    catchup=False,
    tags=["opensky", "bronze", "bigquery"],
) as dag:
    load_task = PythonOperator(
        task_id="load_gcs_to_bq",
        python_callable=load_gcs_to_bq,
    )
