-- staging: bronze의 raw JSON(스냅샷 통짜)을 "항공기 1대 = 1행"으로 펼치고 타입을 부여한다.
-- raw.states = OpenSky state vector 17필드 배열의 배열(docs/opensky-state-vector.md).
--   → BigQuery UNNEST(JSON_QUERY_ARRAY)로 펼치고, JSON_VALUE(aircraft, '$[i]')로 index별 추출 + 캐스팅.
-- 원칙(1:1 정제): 원본 17필드를 전부 살리고 타입·이름만 정리. 필드 취사선택은 marts에서.
--
-- ★ 수집은 전세계(global) raw지만, staging에서 관심 지역(유럽~중동)만 처리(2026-07-22 재설계).
--   "수집은 넓게(bronze 전세계 자산), 분석은 좁게(여기서 관심 큰박스로 필터)".
--   1) 전세계 raw UNNEST → 2) 관심 큰박스(lon -10~70, lat 18~60)만 남김 → 3) 위경도로 region 파생.
--   각 관심 영역 bbox(docs/collection-regions.md) 안에 들면 그 region, 아니면 'other'(관심 큰박스 안 주변 배경).
--   ★ 나중에 다른 주제(태평양·북극 등)는 이 WHERE 범위만 바꾸면 raw에서 재분석(재수집 불필요).

with source as (
    select snapshot_time, raw, _loaded_at    -- bronze.region('eurasia')은 안 씀(아래서 좌표로 파생)
    from {{ source('bronze', 'opensky_states_bronze') }}
),

-- 스냅샷 통짜 → 항공기 N행으로 펼치기(UNNEST). aircraft = 항공기 1대의 17필드 JSON 배열.
exploded as (
    select
        s.snapshot_time,
        s._loaded_at,
        aircraft
    from source as s,
    unnest(json_query_array(s.raw, '$.states')) as aircraft
),

-- 17필드 추출 + 캐스팅. region 파생은 longitude/latitude가 필요하므로 이 단계 뒤에서.
typed as (
    select
        snapshot_time,
        _loaded_at,
        json_value(aircraft, '$[0]')                     as icao24,           -- 0: 항공기 고유 주소(hex)
        nullif(trim(json_value(aircraft, '$[1]')), '')   as callsign,         -- 1: 콜사인(공백 trim, 빈값 NULL)
        json_value(aircraft, '$[2]')                     as origin_country,   -- 2: 등록 국가
        cast(json_value(aircraft, '$[3]') as int64)      as time_position,    -- 3: 마지막 위치 갱신 시각(epoch)
        cast(json_value(aircraft, '$[4]') as int64)      as last_contact,     -- 4: 마지막 신호 수신 시각(epoch)
        cast(json_value(aircraft, '$[5]') as float64)    as longitude,        -- 5: 경도 ★
        cast(json_value(aircraft, '$[6]') as float64)    as latitude,         -- 6: 위도 ★
        cast(json_value(aircraft, '$[7]') as float64)    as baro_altitude,    -- 7: 기압 고도(m)
        cast(json_value(aircraft, '$[8]') as bool)       as on_ground,        -- 8: 지상 여부
        cast(json_value(aircraft, '$[9]') as float64)    as velocity,         -- 9: 속도(m/s)
        cast(json_value(aircraft, '$[10]') as float64)   as true_track,       -- 10: 진행 방향(도, 북=0)
        cast(json_value(aircraft, '$[11]') as float64)   as vertical_rate,    -- 11: 수직 속도(m/s, +상승)
        json_query(aircraft, '$[12]')                    as sensors,          -- 12: 수신 센서 ID 배열(JSON)
        cast(json_value(aircraft, '$[13]') as float64)   as geo_altitude,     -- 13: 기하 고도(m)
        json_value(aircraft, '$[14]')                    as squawk,           -- 14: 트랜스폰더 코드
        cast(json_value(aircraft, '$[15]') as bool)      as spi,              -- 15: 특수목적 지시자
        cast(json_value(aircraft, '$[16]') as int64)     as position_source   -- 16: 위치 출처(0=ADS-B 등)
    from exploded
)

select
    snapshot_time,
    -- region 파생(위경도 → 관심 영역 태깅). bbox는 docs/collection-regions.md와 일치.
    --   ukraine: lon 18~44, lat 42~55 / middle_east: lon 36~66, lat 24~42 / west_europe: lon 2~12, lat 45~52
    --   그 외(큰 박스 안 주변 항공기)는 'other' — 지도 배경으로 "정상 지역 붐빔"을 보여줌.
    case
        when longitude between 18 and 44 and latitude between 42 and 55 then 'ukraine'
        when longitude between 36 and 66 and latitude between 24 and 42 then 'middle_east'
        when longitude between 2  and 12 and latitude between 45 and 52 then 'west_europe'
        else 'other'
    end                                              as region,
    _loaded_at,
    icao24,
    callsign,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    sensors,
    geo_altitude,
    squawk,
    spi,
    position_source
from typed
-- 전세계 raw 중 관심 지역(유럽~중동 큰박스)만 처리. 나머지 전세계는 bronze에 자산으로 남고 여기서 제외.
--   (NULL 좌표는 between이 자동 제외 → 지역 판정 불가 행 걸러짐)
where longitude between -10 and 70
  and latitude  between 18 and 60
