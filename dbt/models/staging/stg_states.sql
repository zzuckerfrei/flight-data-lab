-- staging: bronze의 raw JSON(스냅샷 통짜)을 "항공기 1대 = 1행"으로 펼치고 타입을 부여한다.
-- raw.states = OpenSky state vector 17필드 배열의 배열(docs/opensky-state-vector.md).
--   → BigQuery UNNEST(JSON_QUERY_ARRAY)로 펼치고, JSON_VALUE(aircraft, '$[i]')로 index별 추출 + 캐스팅.
-- 원칙(1:1 정제): 원본 17필드를 전부 살리고 타입·이름만 정리. 필드 취사선택은 marts에서.

with source as (
    select snapshot_time, region, raw, _loaded_at
    from {{ source('bronze', 'opensky_states_bronze') }}
),

-- 스냅샷 통짜 → 항공기 N행으로 펼치기(UNNEST). aircraft = 항공기 1대의 17필드 JSON 배열.
-- alias s를 붙여 "이 컬럼은 source에서 온 것"임을 명시(가독성). aircraft만 unnest 산출물.
exploded as (
    select
        s.snapshot_time,
        s.region,
        s._loaded_at,
        aircraft
    from source as s,
    unnest(json_query_array(s.raw, '$.states')) as aircraft
)

select
    -- 수집 메타
    snapshot_time,
    region,
    _loaded_at,

    -- state vector 17필드 (index 0~16)
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
    json_query(aircraft, '$[12]')                    as sensors,          -- 12: 수신 센서 ID 배열(JSON, 스칼라 아님)
    cast(json_value(aircraft, '$[13]') as float64)   as geo_altitude,     -- 13: 기하 고도(m)
    json_value(aircraft, '$[14]')                    as squawk,           -- 14: 트랜스폰더 코드
    cast(json_value(aircraft, '$[15]') as bool)      as spi,              -- 15: 특수목적 지시자
    cast(json_value(aircraft, '$[16]') as int64)     as position_source   -- 16: 위치 출처(0=ADS-B 등)
from exploded
