-- 완결성 방어선 = 다층 방어 中 [2번: 차단 게이트] "지금 수집이 깨졌나"를 막는 문지기.
-- 목적: '최근' 완결된 날 중 하루 스냅샷 수가 하한 미만이면 FAIL → dbt run 차단.
--   (모든 날의 신뢰도 기록은 [1번: coverage 메타데이터]가 marts에서 별도로 담당)
--
-- ★ 왜 '최근 N일'만? — 품질 게이트 딜레마의 결론(private/learning-notes/data-quality-gate-decisions.md):
--   게이트의 목적은 "복구 가능한 최근 문제"를 막는 것. 과거의 복구 불가능한 구멍까지 막으면
--   → 과거가 미래 mart 생성을 영영 인질로 잡는 '교착'. → 최근 N일만 검사(과거는 coverage 메타로 노출).
--
-- ★ 2026-07-22 수정(큰 박스 재설계 반영): 수집이 4분할→큰 박스 1개(eurasia)로 바뀜.
--   정상 = 144/일(큰 박스 1개 × 10분 = 144스냅샷). 임계 115 = 80%.
--   (옛 기준 461은 옛 4분할 576/일 기준이라, 큰 박스에선 하루 144가 정상인데도 항상 미달 → 오탐 차단.
--    실제 2026-07-21=64로 FAIL해 dbt run이 막힌 incident 발생 → 144 기준으로 교정)
-- ★ 전환 부분일·옛 데이터 제외: 큰 박스 첫 완전한 날(2026-07-22) 이전은 검사 안 함.
--   (7/21은 13:29부터 반나절 수집=64로 부분일이고, 그 이전은 옛 4분할이라 하한 기준 자체가 다름)
-- ★ 당일 제외: 오늘(진행 중)은 아직 쌓이는 중 → 항상 미달이라 false 실패 방지.

{{ config(severity='error') }}

{% set recent_days = 3 %}                          {# 최근 N일: 구멍 나도 사흘 안에 인지·대응 #}
{% set bigbox_first_full_day = '2026-07-22' %}     {# 큰 박스 첫 완전한 날. 이전(전환 부분일·옛 4분할)은 게이트 밖 #}

with daily as (
    select
        date(snapshot_time) as dt,
        count(*)            as snapshot_cnt
    from {{ source('bronze', 'opensky_states_bronze') }}
    -- 검사 범위 = [어제 이전 & 최근 N일 & 큰 박스 완전일 이후]
    where date(snapshot_time) <  current_date('UTC')                                       -- 당일 제외(미완결)
      and date(snapshot_time) >= date_sub(current_date('UTC'), interval {{ recent_days }} day)  -- 과거 교착 방지
      and date(snapshot_time) >= date('{{ bigbox_first_full_day }}')                       -- 전환 부분일·옛 데이터 제외
    group by 1
)

select
    dt,
    snapshot_cnt
from daily
where snapshot_cnt < 115   -- 144(큰 박스 하루)의 80%. 미만이면 '심각한 구멍'으로 판단 → FAIL
order by dt
