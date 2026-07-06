-- bronze 테이블 DDL: OpenSky states raw 스냅샷 (Bronze 레이어)
-- #17 (FD-W201) GCS → BigQuery 적재 대상 테이블.
--
-- 설계(B안): raw 응답을 통짜 JSON으로 불변 보존. 파싱·평탄화는 dbt staging에서.
--   → 메달리온 원칙: bronze = raw 그대로, 변환은 하류(silver/gold).
-- 1행 = 1스냅샷(항공기 여러 대가 든 통짜 JSON). "1행=1항공기" 아님.
--
-- 실행: bq query --use_legacy_sql=false < sql/bronze_opensky_states.sql
-- 생성일: 2026-07-04

CREATE TABLE `flight-data-lab-501011.flight_data.opensky_states_bronze` (
  snapshot_time TIMESTAMP,   -- OpenSky 응답 time 필드(epoch) → TIMESTAMP. 스냅샷 시각
  region        STRING,      -- 수집 영역(파일 경로에서 주입, JSON 본문엔 없음)
  raw           JSON,        -- OpenSky 응답 원본 통짜(불변 보존). dbt가 UNNEST로 파싱
  _loaded_at    TIMESTAMP    -- BQ 적재 시각(관측성·incremental 기준, snapshot_time과 분리)
)
PARTITION BY DATE(snapshot_time);   -- 날짜 파티션 → 파티션 프루닝(스캔량·비용↓)
