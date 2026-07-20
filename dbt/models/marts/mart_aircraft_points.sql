-- marts: 지도(Kepler.gl)용 항공기 위치 점. 분쟁지역 영공 공백·우회를 좌표로 렌더하기 위한 소비 mart.
--   mart_region_density(영역별 밀도 '숫자')와 같은 stg_states에서 파생하되 grain이 다르다:
--     region_density = 1행/영역·스냅샷(집계 숫자, 분석용)  ↔  이 mart = 1행/항공기·스냅샷(좌표 점, 지도용)
--   지도는 개별 좌표가 있어야 점 구름/공백이 보이므로 위경도를 살린 원자 grain으로 만든다.
-- 1행 = 1항공기 × 1스냅샷(30분 버킷). 시간축(snapshot_bucket)으로 재생하면 분포 변화 애니메이션.

with air as (
    select
        snapshot_time,
        region,
        longitude,
        latitude,
        trim(callsign)   as callsign,       -- OpenSky callsign은 공백 패딩 → 트림
        origin_country,
        baro_altitude,
        velocity
    from {{ ref('stg_states') }}
    where on_ground = false                 -- 비행 중만(지상 제외 = 영공 회피 분석 대상)
      and longitude is not null             -- 좌표 없는 행은 지도에 못 찍음
      and latitude is not null
      -- 30분 샘플링: 10분 간격 원본 중 정각/30분 스냅샷만 남긴다(1시간=성김, 10분=과밀의 중간).
      --   버킷(아래 floor)이 '시각 정렬'이라면, 이 필터는 '프레임 솎기'다. 둘 다 해야 30분 간격 프레임.
      --   수집 스케줄이 */10(정각 기반)이라 minute이 0/30인 스냅샷 = 30분 간격.
      and extract(minute from snapshot_time) in (0, 30)
)

select
    -- 30분 버킷 = 애니메이션 프레임 키. 왜 필요?
    --   4영역을 각자 수집해 같은 '00:00'도 영역마다 초 단위로 어긋난다(00:00:07 vs 00:00:11).
    --   그대로 두면 Kepler 시간축이 잘게 쪼개져 프레임이 지저분 → 30분 격자로 내림(floor)해 정렬.
    --   BQ TIMESTAMP_TRUNC은 30분 단위를 지원 안 해 unix초를 1800(=30분)으로 modulo 내림한다.
    timestamp_seconds(unix_seconds(snapshot_time) - mod(unix_seconds(snapshot_time), 1800)) as snapshot_bucket,
    snapshot_time,                          -- 원본 시각(참고·디버깅용)
    region,
    longitude,
    latitude,
    callsign,
    origin_country,
    baro_altitude,                          -- 고도(지도 색상 인코딩 후보)
    velocity                                -- 속도(m/s)
from air
