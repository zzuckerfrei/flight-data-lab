#!/usr/bin/env bash
# flight-data-lab 파이프라인 일일 헬스체크 (읽기 전용)
#
# 수집→적재→변환→배포 전 구간의 상태를 한 번에 실측한다.
# 사실 수집만 하고 원인 판단은 하지 않는다(원인 추정은 사람/에이전트 몫).
#
# 종료 코드: 0=정상 / 1=주의·이상 발견 / 2=점검 전제 미충족(클러스터 미접속 등)
#
# ★ 데이터셋 주의: 운영 산출물은 `marts`다.
#   `dev_marts`(로컬 dbt)·`k8s_dev_marts`(구 k8s dev)는 7월에 멈춘 잔재이므로
#   신선도 판단에 절대 쓰지 않는다. 실제로 이걸 혼동해 장애로 오인한 적이 있다.

set -uo pipefail  # -e는 쓰지 않는다: 한 항목이 실패해도 나머지를 계속 점검해야 한다.

PROJECT="flight-data-lab-501011"
BUCKET="${PROJECT}-bronze"
WEB_URL="https://storage.googleapis.com/${PROJECT}-web/index.html"
BRONZE="${PROJECT}.flight_data.opensky_states_bronze"
MART="${PROJECT}.marts.mart_region_density"   # ★ dev_marts 아님
KCTX="kind-flight-data"

# 수집은 10분 주기 → 20분 넘게 새 파일이 없으면 이상.
MAX_INGEST_LAG_MIN=20
# 하루당기기(logical_date-1): 00:00 UTC 적재는 "어제분"을 넣는다.
# 따라서 bronze·mart의 최신 완결일은 어제(UTC)가 정상이다.
EXPECTED_SNAPS_PER_DAY=144
# 지도는 매일 02:00 UTC 재배포 → 26시간 넘으면 이상.
MAX_MAP_AGE_HOUR=26

WARN=0
section() { printf '\n════ %s ════\n' "$1"; }
ok()   { printf '  [OK]   %s\n' "$1"; }
bad()  { printf '  [!!]   %s\n' "$1"; WARN=1; }
info() { printf '         %s\n' "$1"; }

# UTC 날짜 계산 (BSD/GNU date 양쪽 지원)
utc_date() {  # $1 = 며칠 전(0=오늘)
  date -u -v-"$1"d +"$2" 2>/dev/null || date -u -d "$1 days ago" +"$2"
}
TODAY_C=$(utc_date 0 %Y%m%d)     # GCS 파티션용 (dt=20260808)
YDAY=$(utc_date 1 %Y-%m-%d)      # BQ 완결일 판정용

echo "flight-data-lab 헬스체크 — $(date -u '+%Y-%m-%d %H:%M UTC') / $(date '+%H:%M %Z')"

# ─────────────────────────────────────────────────────────────
section "0. 전제: 클러스터 접속"
if ! kubectl --context "$KCTX" get ns airflow -o name >/dev/null 2>&1; then
  bad "클러스터($KCTX) 접속 불가 — kind 미기동이거나 절전 상태로 보인다."
  info "이 경우 아래 점검은 의미가 없으므로 중단한다."
  exit 2
fi
ok "클러스터 $KCTX 응답"

# ─────────────────────────────────────────────────────────────
section "1. Airflow 코어 파드"
PODS=$(kubectl --context "$KCTX" -n airflow get pods --no-headers 2>/dev/null)
for c in scheduler api-server dag-processor postgresql statsd triggerer quality-exporter; do
  line=$(echo "$PODS" | grep -- "$c" | head -1)
  if [ -z "$line" ]; then
    bad "$c: 파드 없음"
  elif echo "$line" | grep -q "Running"; then
    ok "$c: $(echo "$line" | awk '{print $2, $3}')"
  else
    bad "$c: $(echo "$line" | awk '{print $2, $3}')"
  fi
done

# ─────────────────────────────────────────────────────────────
section "2. DAG 최근 실행"
SCHED=$(kubectl --context "$KCTX" -n airflow get pods -l component=scheduler -o name 2>/dev/null | head -1)
for d in opensky_to_gcs gcs_to_bq dbt_transform deploy_map check_completeness; do
  row=$(kubectl --context "$KCTX" -n airflow exec "$SCHED" -c scheduler -- bash -lc \
        "AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR airflow dags list-runs $d 2>/dev/null" 2>/dev/null \
        | grep -Ei "success|running|failed|queued" | head -1)
  state=$(echo "$row" | awk -F'|' '{gsub(/ /,"",$3); print $3}')
  when=$(echo "$row"  | awk -F'|' '{gsub(/^ +| +$/,"",$5); print substr($5,1,19)}')
  case "$state" in
    success|running|queued) ok  "$(printf '%-19s %-8s %s' "$d" "$state" "$when")" ;;
    "")                     bad "$(printf '%-19s %s' "$d" "실행 이력 조회 실패")" ;;
    *)                      bad "$(printf '%-19s %-8s %s' "$d" "$state" "$when")" ;;
  esac
done

# ─────────────────────────────────────────────────────────────
section "3. GCS 수집 (raw, region=global)"
PREFIX="gs://${BUCKET}/opensky/raw/region=global/dt=${TODAY_C}/"
LISTING=$(gsutil ls -l "$PREFIX" 2>/dev/null | grep "states_")
CNT=$(echo "$LISTING" | grep -c "states_")
if [ "$CNT" -eq 0 ]; then
  bad "오늘(dt=${TODAY_C}) 수집 파일 0건 — 수집 중단 의심"
else
  LATEST_TS=$(echo "$LISTING" | awk '{print $2}' | sort | tail -1)
  ok "오늘 ${CNT}건 적재, 최신 ${LATEST_TS}"
  # 최신 파일이 몇 분 전인지
  LAG=$(python3 - "$LATEST_TS" <<'PY' 2>/dev/null
import sys, datetime
t = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
print(int((datetime.datetime.now(datetime.timezone.utc) - t).total_seconds() // 60))
PY
)
  if [ -n "$LAG" ]; then
    if [ "$LAG" -gt "$MAX_INGEST_LAG_MIN" ]; then
      bad "최신 수집이 ${LAG}분 전 — 10분 주기 대비 지연(임계 ${MAX_INGEST_LAG_MIN}분)"
    else
      ok "수집 지연 ${LAG}분 (정상)"
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────
section "4. BigQuery bronze 완결성 (최근 7일)"
BQ_OUT=$(bq query --project_id="$PROJECT" --use_legacy_sql=false --format=csv "
SELECT DATE(snapshot_time) d, COUNT(DISTINCT snapshot_time) snaps
FROM \`${BRONZE}\`
WHERE DATE(snapshot_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY d ORDER BY d" 2>/dev/null | tail -n +2)
if [ -z "$BQ_OUT" ]; then
  bad "bronze 조회 실패"
else
  while IFS=, read -r d n; do
    [ -z "$d" ] && continue
    if [ "$d" = "$(utc_date 0 %Y-%m-%d)" ]; then
      info "$d ${n}건 (오늘분은 내일 00:00 UTC 적재 — 판정 제외)"
    elif [ "$n" -lt "$EXPECTED_SNAPS_PER_DAY" ]; then
      bad "$d ${n}/${EXPECTED_SNAPS_PER_DAY} — 결측"
    else
      ok "$d ${n}/${EXPECTED_SNAPS_PER_DAY}"
    fi
  done <<< "$BQ_OUT"
  # 어제분이 아예 없으면 적재 DAG 실패 의심
  echo "$BQ_OUT" | grep -q "^${YDAY}," || bad "어제(${YDAY}) 적재분 없음 — gcs_to_bq 확인 필요"
fi

# ─────────────────────────────────────────────────────────────
section "5. mart 신선도 (운영 marts)"
MART_MAX=$(bq query --project_id="$PROJECT" --use_legacy_sql=false --format=csv \
  "SELECT MAX(snapshot_time) FROM \`${MART}\`" 2>/dev/null | tail -1)
if [ -z "$MART_MAX" ]; then
  bad "mart 조회 실패"
else
  MART_DAY="${MART_MAX%% *}"
  if [ "$MART_DAY" = "$YDAY" ]; then
    ok "mart 최신 $MART_MAX (어제분 = 하루당기기 기준 정상)"
  else
    bad "mart 최신 $MART_MAX — 기대값 ${YDAY} (dbt_transform 확인 필요)"
  fi
fi

# ─────────────────────────────────────────────────────────────
section "6. 공개 지도 배포"
LM=$(curl -sI "$WEB_URL" 2>/dev/null | grep -i "^last-modified:" | cut -d' ' -f2- | tr -d '\r')
if [ -z "$LM" ]; then
  bad "지도 응답 없음 — $WEB_URL"
else
  AGE=$(python3 - "$LM" <<'PY' 2>/dev/null
import sys, email.utils, datetime
t = email.utils.parsedate_to_datetime(sys.argv[1])
print(int((datetime.datetime.now(datetime.timezone.utc) - t).total_seconds() // 3600))
PY
)
  if [ -n "$AGE" ] && [ "$AGE" -gt "$MAX_MAP_AGE_HOUR" ]; then
    bad "지도가 ${AGE}시간 전 배포본 (매일 02:00 UTC 갱신 기대, 임계 ${MAX_MAP_AGE_HOUR}h)"
  else
    ok "지도 배포 ${LM} (${AGE:-?}시간 전)"
  fi
fi

# ─────────────────────────────────────────────────────────────
section "판정"
if [ "$WARN" -eq 0 ]; then
  echo "  정상 — 이상 항목 없음"
else
  echo "  이상 — 위 [!!] 항목 확인 필요"
fi
exit "$WARN"
