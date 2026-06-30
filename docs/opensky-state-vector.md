# OpenSky `/states/all` API — 요청·응답·State Vector 스키마

> OpenSky Network REST API의 `/states/all` 엔드포인트 실측 기록.
> 적재(GCS→BigQuery)·변환(dbt) 스키마 설계의 기준 문서.
> 실측일: 2026-06-30 (등록 티어, OAuth2 client credentials)

## 1. 인증 (OAuth2 Client Credentials Flow)

OpenSky는 2026-03-18부터 OAuth2 필수. 인증 서버는 **Keycloak**(`/realms/opensky-network/`).

토큰 발급 (client_id/secret은 파일에서 읽어 셸 히스토리 노출 방지):

```bash
curl -s -X POST "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token" \
  -d grant_type=client_credentials \
  -d client_id=$(jq -r .clientId private/opensky-credential.json) \
  -d client_secret=$(jq -r .clientSecret private/opensky-credential.json) \
  -o private/token.json
```

응답: JWT access token (RS256, Keycloak 서명), `expires_in: 1800` (30분), `token_type: Bearer`.

## 2. API 요청 (`/states/all`, bbox)

```bash
TOKEN=$(jq -r .access_token private/token.json)
curl -s "https://opensky-network.org/api/states/all?lamin=45&lomin=5&lamax=55&lomax=15" \
  -H "Authorization: Bearer $TOKEN"
```

bbox 파라미터:

| 파라미터 | 의미 | 실측 값 |
|---|---|---|
| `lamin` | 위도 하한 | 45 |
| `lomin` | 경도 하한 | 5 |
| `lamax` | 위도 상한 | 55 |
| `lomax` | 경도 상한 | 15 |

→ 서유럽(독일·프랑스·베네룩스 일대). 면적 = (55-45) × (15-5) = **100 sq°**.

## 3. 응답 최상위 구조

```json
{
  "time": 1782816827,
  "states": [ /* 항공기별 state vector 배열, 933개 */ ]
}
```

| 키 | 의미 |
|---|---|
| `time` | 스냅샷 시각 (Unix epoch, 초) |
| `states` | state vector 배열의 배열. 각 원소 = 항공기 1대 (17개 값) |

- 실측: `time=1782816827`, 항공기 **933대**, `X-Rate-Limit-Remaining: 3998`.

## 4. State Vector 구조 (★ 핵심)

**중요**: state vector는 **객체가 아니라 위치(인덱스) 기반 17개 값의 배열**이다. 필드명이 없으므로 적재·변환 시 인덱스→컬럼명 매핑이 필요하다.

실제 첫 항공기 raw (프랑스 Transavia 항공편, 고도 11.6km 순항):

```json
["39de4f","TVF99KE ","France",1782816826,1782816826,6.2264,46.5387,11590.02,false,224.09,309.32,0,null,12169.14,"1000",false,0]
```

### 17필드 매핑 표

| idx | 필드명 | 타입 | 실측 값 | 의미 | 단위 |
|---|---|---|---|---|---|
| 0 | `icao24` | string | `39de4f` | ICAO 24비트 주소(고유 식별자) | hex |
| 1 | `callsign` | string | `TVF99KE ` | 콜사인(항공편명, 공백 패딩 있음) | |
| 2 | `origin_country` | string | `France` | 등록 국가 | |
| 3 | `time_position` | int | 1782816826 | 마지막 위치 갱신 시각 | unix |
| 4 | `last_contact` | int | 1782816826 | 마지막 신호 수신 시각 | unix |
| 5 | `longitude` | float | 6.2264 | **경도** ⭐ | deg |
| 6 | `latitude` | float | 46.5387 | **위도** ⭐ | deg |
| 7 | `baro_altitude` | float | 11590.02 | 기압 고도 | m |
| 8 | `on_ground` | bool | false | 지상 여부 | |
| 9 | `velocity` | float | 224.09 | 대지 속도 | m/s |
| 10 | `true_track` | float | 309.32 | 진행 방향(정북 0°, 시계방향) | deg |
| 11 | `vertical_rate` | float | 0 | 수직 속도(+상승/-하강) | m/s |
| 12 | `sensors` | int[] | null | 수신 센서 ID(본인 수신기일 때만) | |
| 13 | `geo_altitude` | float | 12169.14 | 기하(GPS) 고도 | m |
| 14 | `squawk` | string | `1000` | 트랜스폰더 코드 | |
| 15 | `spi` | bool | false | 특수목적 표시(SPI) | |
| 16 | `position_source` | int | 0 | 위치 출처(0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM) | enum |

(인덱스 17 `category`는 요청 시 `extended=1`을 줘야 포함됨. 이번 응답엔 없음.)

## 5. bbox 동작 검증 (실측)

반환된 933대의 위경도 범위:

| | min | max | 요청 bbox |
|---|---|---|---|
| latitude | 45.0114 | 54.9742 | 45 ~ 55 ✅ |
| longitude | 5.0215 | 14.9578 | 5 ~ 15 ✅ |

→ **bbox 내 항공기만 정확히 반환됨**. 박스 밖 데이터 없음.

## 6. 크레딧 (실측)

- 등록(Standard) 티어: **4,000 크레딧/일, Daily refill**(매일 충전, 일회성 아님).
- 버킷 독립: `/states/*`, `/tracks/*`, `/flights/*` 각각 별도 → `/states`만 쓰면 4000 전부 사용 가능.
- **bbox 면적별 크레딧**: ≤25 sq°→1 / 25~100→2 / 100~400→3 / 전역→4.
  - 실측: 100 sq° 호출 1회 → `Remaining` 4000→3998 = **2크레딧** 소모 확인.
  - 면적 = (lamax-lamin) × (lomax-lomin).
- 소진 시 `429 Too Many Requests` + `X-Rate-Limit-Retry-After-Seconds` 헤더.
- 🟡 정확한 리셋 시각(UTC 자정 vs rolling 24h)은 공식 문서 불명확 → 실측 확인 예정.

## 7. 우리 프로젝트(영공 밀도) 설계 노트

- **핵심 필드**: `longitude`·`latitude`·`time`(스냅샷 시각). 지도·밀도 분석의 전부. 나머지(고도·속도·국가)는 보조.
- **메달리온 매핑**:
  - **Bronze(GCS)**: raw JSON 그대로 보존(배열 형태, 불변).
  - **Silver(dbt staging)**: 인덱스→컬럼명 부여 + 타입 캐스팅(unix→timestamp, string/float/bool), 영역 태깅.
  - **Gold(dbt marts)**: 영역·시간대별 밀도 집계.
- **null 처리**: `sensors`는 보통 null. `callsign`·`squawk` 등도 결측 가능 → nullable 컬럼.
- **수집 스키마 주의**: callsign 공백 패딩(trim 필요), squawk는 string(숫자 아님), 좌표 결측 행 존재 가능(필터).

## 참고
- OpenSky REST API 공식: https://openskynetwork.github.io/opensky-api/rest.html
- 토큰 endpoint: https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token
