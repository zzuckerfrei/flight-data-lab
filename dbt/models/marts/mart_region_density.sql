-- marts: 관심 영역·스냅샷별 항공기 밀도(면적 정규화). 분쟁지역 영공 회피의 정량 지표.
--   density = 공중 항공기 수 / 영역 면적(sq°) → 면적 다른 영역을 공정 비교.
--   staging을 ref()로 참조 → dbt가 "stg_states 다음 이 marts" 실행 순서 자동 인식.
-- 1행 = 1스냅샷 × 1관심영역의 밀도. 시간축(snapshot_time)으로 뽑으면 밀도 시계열.
--
-- ★ 2026-07-21 재설계: 수집이 큰 박스 1개(eurasia)로 바뀜.
--   - region은 stg_states에서 위경도로 파생(ukraine/middle_east/west_europe/other).
--   - 밀도는 관심 3영역만 계산(other=주변 배경은 면적이 크고 불규칙 → 밀도 지표에서 제외, 지도 배경으로만).
--   - coverage는 (날짜×영역)이 아니라 날짜 단위(수집이 큰 박스 하나라 완결도도 하나).

with states as (
    select snapshot_time, region, on_ground
    from {{ ref('stg_states') }}
    where on_ground = false          -- 영공 회피 분석: 비행 중 항공기만(지상 제외)
      and region != 'other'          -- 관심 3영역만 밀도 계산(other=주변 배경 제외)
),

-- region_area = 관심 영역별 bbox 면적(sq°) 참조 seed.
--   면적 = (lamax-lamin)*(lomax-lomin). stg_states의 region 파생 bbox와 일치(docs/collection-regions.md).
--   other는 seed에 없음 → inner join으로 자동 drop(밀도 계산에서 빠짐).
region_area as (
    select region, area_sqdeg
    from {{ ref('region_area') }}
),

-- coverage 메타데이터 = 다층 방어 中 [1번: 기록/투명성]. "이 밀도값이 얼마나 완전한 관측에서 나왔나".
--   수집이 큰 박스 1개라 스냅샷은 시각당 하나 → 날짜별 distinct 스냅샷 수 / 144(하루 기대)로 계산(region 무관).
--   ★ on_ground 필터 전 stg_states 전체(other 포함) 기준이어야 '수집 완결도'가 맞음.
daily_coverage as (
    select
        date(snapshot_time)                             as dt,
        count(distinct snapshot_time)                   as snapshots_observed,   -- 그날 실제 스냅샷 수
        round(count(distinct snapshot_time) / 144.0, 3) as coverage_pct          -- 완결도(1.0=144 다 옴)
    from {{ ref('stg_states') }}
    group by 1
)

select
    s.snapshot_time,
    s.region,
    count(*)                                    as aircraft_count,      -- 공중 항공기 수
    a.area_sqdeg,
    round(count(*) / a.area_sqdeg, 4)           as density,             -- 면적 정규화 밀도 ★
    c.coverage_pct                                                      -- 그날 수집 완결도(신뢰도 메타)
from states as s
join region_area as a using (region)
-- coverage는 날짜 단위 → 같은 날 모든 행에 같은 값(denormalized, 소비 편의). region 조건 없음.
join daily_coverage as c on c.dt = date(s.snapshot_time)
group by s.snapshot_time, s.region, a.area_sqdeg, c.coverage_pct
