"""완결성 모니터링 DAG (#49, B1 알림 경로).

volume 게이트를 warn으로 낮춘 뒤(2026-07-23, 차단 안 함=교착 회피), 완결성 부족을 여기서 별도로
감지해 Slack 경고한다. "감지/알림"을 "dbt 변환·차단"과 분리 —
부족해도 파이프라인은 진행(marts 생성)하되, 담당자는 이 경고로 인지한다.

★ 왜 별도 DAG? dbt severity=warn은 exit 0(task success)이라 on_failure_callback이 안 불림
  → warn이 Slack까지 안 옴. 그래서 완결성 부족을 BQ로 직접 조회해 알리는 경로를 분리한다.
  (규모 커지면 Elementary/메트릭+Grafana로 중앙화 → 이슈 #49)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

from utils.slack_alerts import notify_failure  # 이 DAG 자체 실패(BQ 오류 등) 알림 (#33)

PROJECT = "flight-data-lab-501011"
TABLE = f"{PROJECT}.flight_data.opensky_states_bronze"
THRESHOLD = 115      # 정상 144/일(1영역 × 10분)의 80%. volume 게이트와 동일 기준.
RECENT_DAYS = 3


def check_and_alert(**context):
    """최근 N일 중 스냅샷 부족한 날을 BQ로 조회 → 있으면 Slack 경고(차단 아님)."""
    from google.cloud import bigquery

    bq = bigquery.Client(project=PROJECT)
    query = f"""
        SELECT DATE(snapshot_time) AS dt, COUNT(*) AS snapshot_cnt
        FROM `{TABLE}`
        WHERE DATE(snapshot_time) <  CURRENT_DATE('UTC')                                       -- 당일 제외(진행 중)
          AND DATE(snapshot_time) >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL {RECENT_DAYS} DAY) -- 최근 N일
        GROUP BY dt
        HAVING snapshot_cnt < {THRESHOLD}
        ORDER BY dt
    """
    rows = list(bq.query(query).result())

    if not rows:
        print(f"완결성 정상 (최근 {RECENT_DAYS}일 모두 >= {THRESHOLD})")
        return

    # 부족한 날이 있으면 경고(차단 아님 — marts는 이미 생성됨, coverage_pct로 완결도 노출).
    detail = ", ".join(f"{r.dt}={r.snapshot_cnt}/{THRESHOLD}" for r in rows)
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "⚠️ 데이터 완결성 경고", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": (
            f"최근 {RECENT_DAYS}일 중 bronze 스냅샷 부족(정상 144/일 기준):\n*{detail}*\n\n"
            "→ *차단 아님* (marts는 생성됨, `coverage_pct`로 완결도 확인). "
            "외부 장애(예: OpenSky 503)나 수집 이상 여부 점검 필요."
        )}},
    ]
    SlackWebhookHook(slack_webhook_conn_id="slack_default").send(
        text=f"⚠️ 완결성 경고: {detail}", blocks=blocks
    )
    print(f"완결성 경고 발송: {detail}")


with DAG(
    dag_id="check_completeness",
    # dbt_transform(01:00)·deploy_map(02:00) 이후 02:30 → "어제 완결성 괜찮았나" 하루 1회 체크.
    schedule="30 2 * * *",
    start_date=datetime(2026, 7, 23),
    catchup=False,
    tags=["monitoring", "quality"],
    default_args={"on_failure_callback": notify_failure},
) as dag:
    PythonOperator(
        task_id="check_completeness",
        python_callable=check_and_alert,
    )
