"""자동배포용: BQ marts → Kepler.gl HTML → GCS 정적 웹 (#45).

Airflow가 kepler-map 이미지 Pod로 이 스크립트를 주기 실행:
  1) BQ marts.mart_aircraft_points 최근 N일(관심지역) 조회
  2) keplergl HTML 생성(build_map.py와 동일 config: color region·dark-matter·1h 윈도우·bbox 강조)
  3) GCS 웹 버킷 index.html 업로드 → 공개 URL 자동 갱신

인증: SA키를 런타임 k8s Secret 볼륨으로 주입(GOOGLE_APPLICATION_CREDENTIALS). 이미지엔 안 굽는다(dbt 패턴).
환경변수:
  MAP_DAYS       최근 며칠 (기본 3)
  BQ_PROJECT     기본 flight-data-lab-501011
  WEB_BUCKET     기본 flight-data-lab-501011-web
"""
import os
import pandas as pd
from google.cloud import bigquery, storage
from keplergl import KeplerGl

PROJECT = os.environ.get("BQ_PROJECT", "flight-data-lab-501011")
WEB_BUCKET = os.environ.get("WEB_BUCKET", "flight-data-lab-501011-web")
DAYS = int(os.environ.get("MAP_DAYS", "3"))
OUT = "/tmp/index.html"

# ── 1) BQ 조회 (최근 N일 관심지역). region은 marts에 이미 파생돼 있음(ukraine/middle_east/west_europe/other) ──
bq = bigquery.Client(project=PROJECT)
query = f"""
SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', snapshot_bucket) AS ts,
       region, longitude, latitude, callsign, origin_country, baro_altitude, velocity
FROM `{PROJECT}.marts.mart_aircraft_points`
WHERE DATE(snapshot_bucket) >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL {DAYS} DAY)
ORDER BY snapshot_bucket
"""
df = bq.query(query).to_dataframe()
print(f"BQ rows: {len(df)} (최근 {DAYS}일)")

if df.empty:
    # 최근 N일 데이터가 없으면(수집 공백 등) 기존 지도 유지 — 빈 지도로 덮어쓰지 않음.
    raise SystemExit("최근 데이터 없음 → 배포 건너뜀(기존 index.html 유지)")

# ── 2) keplergl HTML (build_map.py와 동일 config) ──
# 관심 영역 3개 bbox 테두리(강조). stg_states region 파생 좌표와 동일.
REGION_BBOX = {
    "ukraine":     {"lamin": 42, "lamax": 55, "lomin": 18, "lomax": 44},
    "middle_east": {"lamin": 24, "lamax": 42, "lomin": 36, "lomax": 66},
    "west_europe": {"lamin": 45, "lamax": 52, "lomin": 2,  "lomax": 12},
}


def _bbox_feature(region, b):
    lo1, lo2, la1, la2 = b["lomin"], b["lomax"], b["lamin"], b["lamax"]
    return {
        "type": "Feature",
        "properties": {"region": region},
        "geometry": {"type": "Polygon",
                     "coordinates": [[[lo1, la1], [lo2, la1], [lo2, la2], [lo1, la2], [lo1, la1]]]},
    }


regions_geojson = {"type": "FeatureCollection",
                   "features": [_bbox_feature(r, b) for r, b in REGION_BBOX.items()]}

# 초기 1시간 슬라이딩 윈도우(ts는 UTC).
_ts_min = pd.to_datetime(df["ts"], utc=True).min()
_start_ms = int(_ts_min.timestamp() * 1000)
_window_ms = 60 * 60 * 1000

config = {
    "version": "v1",
    "config": {
        "visState": {
            "filters": [{
                "dataId": ["aircraft"], "id": "ts_time", "name": ["ts"], "type": "timeRange",
                "value": [_start_ms, _start_ms + _window_ms], "enlarged": True,
                "plotType": "histogram", "animationWindow": "free", "speed": 1,
            }],
            "layers": [
                {
                    "id": "aircraft_pt", "type": "point",
                    "config": {
                        "dataId": "aircraft", "label": "aircraft",
                        "columns": {"lat": "latitude", "lng": "longitude", "altitude": None},
                        "isVisible": True,
                        "visConfig": {
                            "radius": 4, "opacity": 0.6,
                            "colorRange": {  # middle_east 주황 / other 회색 / ukraine 빨강 / west_europe 파랑
                                "name": "Custom Region", "type": "custom", "category": "Custom",
                                "colors": ["#ff7f00", "#888888", "#e41a1c", "#377eb8"],
                            },
                        },
                    },
                    "visualChannels": {
                        "colorField": {"name": "region", "type": "string"},
                        "colorScale": "ordinal",
                    },
                },
                {
                    "id": "region_boxes", "type": "geojson",
                    "config": {
                        "dataId": "regions", "label": "collection bbox",
                        "columns": {"geojson": "_geojson"}, "isVisible": True,
                        "visConfig": {"filled": False, "stroked": True,
                                      "strokeColor": [255, 255, 255], "thickness": 1.5, "strokeOpacity": 0.8},
                    },
                },
            ],
        },
        "mapState": {"latitude": 42.0, "longitude": 35.0, "zoom": 3.2},
        "mapStyle": {"styleType": "dark-matter"},
    },
}

m = KeplerGl(height=700, data={"aircraft": df, "regions": regions_geojson}, config=config)
m.save_to_html(file_name=OUT, config=config)

# ★ 마운트 크기 오측정 버그 수정 (#46 계열).
#   원인: Kepler가 #app 컨테이너를 AutoSizer로 재서 지도 캔버스 크기를 정하는데, save_to_html
#   기본 HTML엔 컨테이너 height CSS가 없어 로드 시점에 '부분 크기'로 측정됨 → 지도가 작게 렌더.
#   'switch to dual map view' 토글이나 창 리사이즈가 resize 이벤트를 쏘면 재측정돼 펴졌음(방문자 조작 필요).
#   해결: <head>에 (1) 뷰포트 크기 CSS + (2) load 후 resize 자동 발생 스크립트 주입.
#   ★ JS를 </body>에 주입하면 임베드 번들이 깨졌던 이력 → head에만 주입(안전).
_FIX = (
    '<style>html,body{margin:0;padding:0;width:100%;height:100%;overflow:hidden}'
    '#app{position:absolute;top:0;left:0;width:100%;height:100%}</style>'
    '<script>window.addEventListener("load",function(){'
    'setTimeout(function(){window.dispatchEvent(new Event("resize"))},300)})</script>'
)
with open(OUT, "r", encoding="utf-8") as _f:
    _html = _f.read()
_html = _html.replace("</head>", _FIX + "</head>", 1)
with open(OUT, "w", encoding="utf-8") as _f:
    _f.write(_html)
print(f"HTML 생성 + 마운트크기 수정 주입: {OUT}")

# ── 3) GCS 웹 버킷 업로드 (index.html) ──
gcs = storage.Client(project=PROJECT)
blob = gcs.bucket(WEB_BUCKET).blob("index.html")
blob.upload_from_filename(OUT, content_type="text/html")
print(f"배포 완료: gs://{WEB_BUCKET}/index.html (rows={len(df)})")
