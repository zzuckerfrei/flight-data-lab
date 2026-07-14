-- 완결성 방어선 = 다층 방어 中 [2번: 차단 게이트] "지금 수집이 깨졌나"를 막는 문지기.
-- 목적: '최근' 완결된 날 중 하루 스냅샷 수가 하한 미만이면 FAIL → dbt run 차단.
--   (모든 날의 신뢰도 기록은 [1번: coverage 메타데이터]가 marts에서 별도로 담당)
--
-- ★ 왜 '최근 N일'만? — 품질 게이트 딜레마의 결론(private/learning-notes/data-quality-gate-decisions.md):
--   게이트의 목적은 "복구 가능한 최근 문제"를 막는 것. 과거의 복구 불가능한 구멍(예: 7/5·7/7,
--   수집 자체가 없어 GCS에도 없음)까지 막으면 → 과거가 미래 mart 생성을 영영 인질로 잡는 '교착'.
--   → 최근 N일만 검사. 과거 구멍은 게이트 밖(대신 coverage 메타로 투명하게 노출).
--
-- 기준(실측): 정상 = 영역별 144/일 × 4영역 = 576/일(100%). 임계 461 = 80%.
--   명백한 구멍(58%·69%)은 잡고, 미세 손실(98%)은 통과.
-- ★ 당일 제외: 오늘(진행 중)은 아직 쌓이는 중 → 항상 미달이라 false 실패 방지.

{{ config(severity='error') }}

{% set recent_days = 3 %}   {# 최근 N일: 구멍 나도 사흘 안에 인지·대응 #}

with daily as (
    select
        date(snapshot_time) as dt,
        count(*)            as snapshot_cnt
    from {{ source('bronze', 'opensky_states_bronze') }}
    -- 검사 범위 = [어제 이전 & 최근 N일]: 당일 제외(미완결) + 과거 교착 방지
    where date(snapshot_time) <  current_date('UTC')
      and date(snapshot_time) >= date_sub(current_date('UTC'), interval {{ recent_days }} day)
    group by 1
)

select
    dt,
    snapshot_cnt
from daily
where snapshot_cnt < 461   -- 576의 80%. 미만이면 '심각한 구멍'으로 판단 → FAIL
order by dt
