-- marts: 격자 밀도 소비용 gold 테이블. Kepler 히트맵·Grafana 추세가 직접 읽는다.
--   int_grid_density(중간 격자 집계) + 날짜별 coverage_pct(완결도 메타)를 붙여
--   소비자가 "이 셀 값이 얼마나 완전한 관측에서 나왔나"까지 판단할 수 있게 한다.
-- grain = 1행 = (날짜 × 셀 × region). 지도는 최근일 필터, 추세는 dt로 시계열 조회.

with grid as (
    select * from {{ ref('int_grid_density') }}
),

-- 완결도(다층 방어 中 기록/투명성): mart_region_density와 동일 정의(날짜 단위, region 무관).
daily_coverage as (
    select
        date(snapshot_time)                             as dt,
        round(count(distinct snapshot_time) / 144.0, 3) as coverage_pct
    from {{ ref('stg_states') }}
    group by 1
)

select
    g.dt,
    g.region,
    g.cell_lon,
    g.cell_lat,
    g.aircraft_count,
    c.coverage_pct
from grid as g
join daily_coverage as c using (dt)
