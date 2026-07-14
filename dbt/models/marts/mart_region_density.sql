-- marts: 영역·스냅샷별 항공기 밀도(면적 정규화). 분쟁지역 영공 회피의 정량 지표.
--   density = 공중 항공기 수 / 영역 면적(sq°) → 면적 다른 영역을 공정 비교(우크라162 vs 한국48).
--   staging을 ref()로 참조 → dbt가 "stg_states 다음 이 marts" 실행 순서 자동 인식.
-- 1행 = 1스냅샷 × 1영역의 밀도. 시간축(snapshot_time)으로 뽑으면 밀도 시계열.

with states as (
    select snapshot_time, region, on_ground
    from {{ ref('stg_states') }}
    where on_ground = false          -- 영공 회피 분석: 비행 중 항공기만(지상 제외)
),

-- region_area = 영역별 bbox 면적(sq°) 참조 테이블.
--   무엇: region(지역명) → area_sqdeg(면적) 매핑. 면적 = (lamax-lamin)*(lomax-lomin), docs/collection-regions.md.
--   왜:   밀도 = 항공기수 / 면적 으로 정규화하려면 각 지역의 면적이 필요한데,
--         이 값은 실데이터가 아니라 우리가 정한 수집 영역 상수라 seed(참조 CSV)로 관리한다.
--         (seeds/region_area.csv → dbt seed로 BQ 테이블화. 지역 추가/변경 시 CSV만 고치면 됨.)
region_area as (
    select region, area_sqdeg
    from {{ ref('region_area') }}    -- seed 참조(인라인 union all → CSV로 분리)
),

-- coverage 메타데이터 = 다층 방어 中 [1번: 기록/투명성]. "이 밀도값이 얼마나 완전한 관측에서 나왔나".
--   완결성 게이트(2번)가 '최근 구멍을 차단'한다면, 여기선 '모든 날의 신뢰도를 데이터로 남긴다'.
--   구멍을 숨기거나 막는 게 아니라, coverage%로 투명하게 노출 → 소비자(지도·분석)가 신뢰도 판단.
--   상세 결정 배경: private/learning-notes/data-quality-gate-decisions.md
-- 계산: (날짜×영역)별 실제 스냅샷 수 / 144(하루 기대: 10분 간격 × 144). stg_states 전체(지상 포함) 기준.
--   ★ on_ground 필터 전 stg_states를 봐야 '수집 완결도'가 맞음(밀도용 states는 비행 중만이라 부적합).
daily_coverage as (
    select
        date(snapshot_time)                      as dt,
        region,
        count(distinct snapshot_time)            as snapshots_observed,   -- 그날 그영역 실제 스냅샷 수
        round(count(distinct snapshot_time) / 144.0, 3) as coverage_pct   -- 완결도(1.0=576/4=144 다 옴)
    from {{ ref('stg_states') }}
    group by 1, 2
)

select
    s.snapshot_time,
    s.region,
    count(*)                                    as aircraft_count,      -- 공중 항공기 수
    a.area_sqdeg,
    round(count(*) / a.area_sqdeg, 4)           as density,             -- 면적 정규화 밀도 ★
    c.coverage_pct                                                      -- 그날 그영역 수집 완결도(신뢰도 메타)
from states as s
join region_area as a using (region)
-- coverage는 (날짜×영역) 단위 → 같은 날 모든 스냅샷 행에 같은 값(denormalized, 소비 편의)
join daily_coverage as c
    on c.dt = date(s.snapshot_time) and c.region = s.region
group by s.snapshot_time, s.region, a.area_sqdeg, c.coverage_pct
