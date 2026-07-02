# OpenSky 응답 샘플 (스키마 참고용)

`opensky_to_gcs` DAG(#18)가 GCS bronze에 적재한 raw JSON 스냅샷을 영역별 1건씩 보존한 것.
실제로 어떤 데이터가 들어오는지 잊지 않기 위한 참고 자료다(파이프라인 입력 X).

- **출처**: OpenSky `/states/all`, bbox로 영역 한정 → GCS `opensky/raw/region=.../dt=.../states_<epoch>.json`
- **스냅샷 시각**: 2026-07-02 12:10 UTC 전후 (epoch `178299420x`)
- **영역 4곳**: ukraine · middle_east(분쟁) / west_europe · korea(비교) — bbox는 [`../../collection-regions.md`](../collection-regions.md)

## 구조

최상위 = 2키:

| 키 | 의미 |
|---|---|
| `time` | 스냅샷 시각 (unix epoch) |
| `states` | 항공기 배열. 각 원소가 **17필드 배열**(객체 아님) |

`states[i]`는 위치 기반 배열이라 index로 읽어야 한다. 예: `[5]`=경도, `[6]`=위도, `[0]`=icao24.
**17필드 전체 매핑**은 [`../opensky-state-vector.md`](../opensky-state-vector.md) 참고.

> 배열→컬럼명+타입 캐스팅은 dbt staging 레이어에서 수행한다.

## 파일

| 파일 | 영역 | 대략 항공기 수 |
|---|---|---|
| `ukraine.json` | 우크라이나+흑해 | ~수십 대 |
| `middle_east.json` | 중동 | ~수십 대 |
| `west_europe.json` | 서유럽 | ~수백 대 (가장 큼) |
| `korea.json` | 한국 | ~백 대 |
