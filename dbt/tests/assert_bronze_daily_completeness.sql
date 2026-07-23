-- 완결성 방어선 = 다층 방어 中 [경고 게이트]. "수집이 얼마나 완전했나"를 알린다(★차단 아님, WARN).
--
-- ★ 2026-07-23 B1 재설계 (hard error → warn):
--   계기: OpenSky 503 장애(2026-07-22, 5.8h 공백)로 7/22=75<115 → 게이트가 error로 dbt_transform을
--         교착시킨 incident(private/incidents/2026-07-22-opensky-api-503-outage.md).
--   딜레마: 외부 장애로 인한 '복구 불가능한 구멍'(OpenSky는 과거 1h만 제공 → 놓친 날은 영영 못 채움)이
--           최근 N일 창에 들어오면, hard 차단은 "못 고치는 과거가 미래 mart 생성을 인질로 잡는" 교착이 된다.
--   현업 원칙: 게이트 목적 = 다운스트림 오염 방지. 완결성 '부족'은 차단이 아니라 관측 대상이다.
--     - 감지/기록: coverage_pct(marts에 그날 완결도 % 투명 노출) — 이미 존재.
--     - 경고: 이 test를 WARN으로 → 담당자는 dbt WARN + Slack + coverage로 부족을 인지.
--     - 차단(hard)은 정말 오염되는 것만(예: 스냅샷 0=수집 완전 죽음, 스키마 붕괴) — 별도/추후.
--   → "감지(coverage)와 차단을 분리": 게이트가 다 짊어지지 않고, 부족은 노출하되 파이프라인은 진행(교착 회피).
--
-- 기준: 정상 = 144/일(큰박스·전세계 1영역 × 10분). 임계 115 = 80%. 미만이면 WARN.
-- 당일 제외(진행 중이라 항상 미달) + 최근 N일만(과거 구멍 노이즈 억제).

{{ config(severity='warn') }}

{% set recent_days = 3 %}   {# 최근 N일: 구멍 나도 사흘 안에 인지 #}

with daily as (
    select
        date(snapshot_time) as dt,
        count(*)            as snapshot_cnt
    from {{ source('bronze', 'opensky_states_bronze') }}
    where date(snapshot_time) <  current_date('UTC')                                      -- 당일 제외
      and date(snapshot_time) >= date_sub(current_date('UTC'), interval {{ recent_days }} day)  -- 최근 N일만
    group by 1
)

select
    dt,
    snapshot_cnt
from daily
where snapshot_cnt < 115   -- 144의 80%. 미만이면 WARN(관측·알림, 파이프라인은 그대로 진행)
order by dt
