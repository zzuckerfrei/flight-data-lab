"""데이터 품질 메트릭 exporter (#50, 층 B).

BQ를 주기적으로 조회해 데이터 품질 지표를 Prometheus 게이지로 노출한다.
- 백그라운드 스레드가 REFRESH_INTERVAL(기본 300초)마다 BQ 1쿼리 조회 → 게이지 set.
- HTTP :8000 /metrics 는 Prometheus가 1분마다 긁음(메모리 게이지만 반환, BQ 안 침).
- 조회 주기 ≠ scrape 주기 분리 → BQ 비용 통제(설계 근거 ADR-0002).

지표(전부 Gauge):
  flight_bronze_snapshots_total{date}  일별 스냅샷 수(완결성). 정상 144.
  flight_coverage_ratio{region}        region별 완결도(0~1).
  flight_bronze_freshness_seconds      마지막 적재 후 경과 초(신선도).
  flight_region_density{region}        region별 밀도(anomaly 관측).
  flight_exporter_query_success        마지막 BQ 조회 성공 여부(1/0). exporter 자체 관측.
  flight_exporter_last_success_timestamp  마지막 성공 조회 epoch(스테일 감지용).
"""
import os
import time
import threading

from prometheus_client import start_http_server, Gauge
from google.cloud import bigquery

PROJECT = os.environ.get("BQ_PROJECT", "flight-data-lab-501011")
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "300"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8000"))

# 4지표를 롱포맷(kind/label/value)으로 합쳐 1쿼리 = BQ 최소 과금(10MB) 1회.
# 모든 파트에 파티션(snapshot_time) 조건 → 스캔 최소화.
QUERY = f"""
SELECT 'snapshot' AS kind, CAST(DATE(snapshot_time) AS STRING) AS label,
       CAST(COUNT(*) AS FLOAT64) AS value
FROM `{PROJECT}.flight_data.opensky_states_bronze`
WHERE DATE(snapshot_time) >= CURRENT_DATE()-3
GROUP BY 1, 2
UNION ALL
SELECT 'freshness', '',
       CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_loaded_at), SECOND) AS FLOAT64)
FROM `{PROJECT}.flight_data.opensky_states_bronze`
WHERE DATE(snapshot_time) >= CURRENT_DATE()-1
UNION ALL
SELECT 'coverage', region, CAST(AVG(coverage_pct) AS FLOAT64)
FROM `{PROJECT}.marts.mart_region_density`
WHERE DATE(snapshot_time) = (
    SELECT MAX(DATE(snapshot_time)) FROM `{PROJECT}.marts.mart_region_density`)
GROUP BY region
UNION ALL
SELECT 'density', region, CAST(AVG(density) AS FLOAT64)
FROM `{PROJECT}.marts.mart_region_density`
WHERE DATE(snapshot_time) = (
    SELECT MAX(DATE(snapshot_time)) FROM `{PROJECT}.marts.mart_region_density`)
GROUP BY region
"""

g_snapshots = Gauge("flight_bronze_snapshots_total", "일별 bronze 스냅샷 수(완결성)", ["date"])
g_coverage = Gauge("flight_coverage_ratio", "region별 완결도(0~1)", ["region"])
g_freshness = Gauge("flight_bronze_freshness_seconds", "마지막 적재 후 경과 초(신선도)")
g_density = Gauge("flight_region_density", "region별 항공기 밀도", ["region"])
g_query_success = Gauge("flight_exporter_query_success", "마지막 BQ 조회 성공(1)/실패(0)")
g_last_success = Gauge("flight_exporter_last_success_timestamp", "마지막 성공 조회 epoch")

_client = None


def _get_client():
    # 클라이언트 지연 생성(SA키 볼륨 마운트 완료 후 첫 조회 시점에).
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT)
    return _client


def refresh():
    """BQ 1쿼리 조회 → 게이지 갱신. date/region label은 clear 후 재설정(스테일 방지)."""
    rows = list(_get_client().query(QUERY).result())
    # 라벨 누적(어제 날짜가 창 밖으로 나가도 남는 문제) 방지: 매 조회마다 초기화.
    g_snapshots.clear()
    g_coverage.clear()
    g_density.clear()
    for r in rows:
        if r.value is None:
            continue
        if r.kind == "snapshot":
            g_snapshots.labels(date=r.label).set(r.value)
        elif r.kind == "coverage":
            g_coverage.labels(region=r.label).set(r.value)
        elif r.kind == "freshness":
            g_freshness.set(r.value)
        elif r.kind == "density":
            g_density.labels(region=r.label).set(r.value)


def loop():
    while True:
        try:
            refresh()
            g_query_success.set(1)
            g_last_success.set(time.time())
        except Exception as e:  # 조회 실패해도 죽지 않고 다음 주기 재시도(관측만 남김)
            g_query_success.set(0)
            print(f"[exporter] BQ 조회 실패: {e}", flush=True)
        time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    start_http_server(HTTP_PORT)  # /metrics 노출
    print(f"[exporter] :{HTTP_PORT}/metrics 시작, refresh={REFRESH_INTERVAL}s, project={PROJECT}", flush=True)
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    while True:
        time.sleep(3600)
