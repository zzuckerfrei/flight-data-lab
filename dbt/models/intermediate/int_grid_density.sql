-- intermediate: staging의 항공기 점을 정사각 격자 셀로 버킷팅해 (날짜 × 셀 × region)별 대수를 집계한다.
--   staging → intermediate → marts medallion의 '중간 재사용 층'(#25).
--   재사용하려고 뺐는데 실제로 쓰는 건 mart_grid_density 한 곳뿐이다. 합칠지는 #52 참고.
-- ★ 왜 격자 count가 곧 밀도인가: 셀 크기가 균일(기본 0.5°×0.5°)이라 셀 간 대수 비교 = 밀도 비교.
--   region 단위 밀도(mart_region_density)가 면적으로 나눈 이유(박스 크기가 제각각)와 대비 —
--   격자는 셀이 균일해 분모(면적)가 불필요하다.
-- 셀 크기는 var(grid_cell_deg)로 조정 가능(0.5 → 0.25면 4배 촘촘). 재수집 없이 해상도만 변경.

with flying as (
    select
        date(snapshot_time) as dt,
        region,
        longitude,
        latitude
    from {{ ref('stg_states') }}
    where on_ground = false          -- 영공 회피 분석: 비행 중 항공기만(지상 제외)
      and longitude is not null
      and latitude  is not null
),

-- 각 점을 셀 좌측하단 모서리 좌표로 스냅(FLOOR). 같은 셀에 든 점들은 동일 (cell_lon, cell_lat).
celled as (
    select
        dt,
        region,
        floor(longitude / {{ var('grid_cell_deg') }}) * {{ var('grid_cell_deg') }} as cell_lon,
        floor(latitude  / {{ var('grid_cell_deg') }}) * {{ var('grid_cell_deg') }} as cell_lat
    from flying
)

select
    dt,
    region,
    cell_lon,
    cell_lat,
    count(*) as aircraft_count        -- 셀당 항공기 관측 수(= 균일 격자라 밀도 그 자체)
from celled
group by dt, region, cell_lon, cell_lat
