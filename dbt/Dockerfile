# dbt 실행 전용 이미지 (Cosmos Kubernetes 실행 모드용).
# dbt Pod가 이 이미지로 떠서 자족적으로 `dbt run/test`를 실행한다.
# 담는 것: dbt 프로젝트(models·seeds·macros) + profiles.yml + dbt-bigquery 어댑터 + dbt_packages.
# 담지 않는 것: SA 키(런타임에 k8s Secret으로 /root/.dbt/gcp-sa-key.json 마운트).
# 참고: Cosmos 공식 예제 Dockerfile.postgres_profile_docker_k8s 패턴.
#
# 빌드:      docker build -t dbt-flight:1.0.0 -f Dockerfile .
# kind 주입:  kind load docker-image dbt-flight:1.0.0 --name flight-data
FROM python:3.12-slim

# dbt-bigquery(어댑터) 설치 — dbt-core는 의존성으로 딸려온다.
# 버전 고정: 로컬 개발(1.11.3)과 이미지를 일치시켜 재현성 확보("이미지선 다른 버전" 방지).
RUN pip install --no-cache-dir "dbt-bigquery==1.11.3"

# profiles.yml을 Cosmos 표준 경로(/root/.dbt/)에 배치. ← dbt Pod가 실제로 쓰는 profiles.
# K8s 실행 모드는 profile 자동생성을 안 하므로 이미지에 직접 넣어야 한다.
RUN mkdir -p /root/.dbt
COPY profiles.yml /root/.dbt/profiles.yml

WORKDIR /usr/app/dbt

# 캐시 최적화 + 재현성: 패키지 정의만 먼저 복사해 deps 설치.
#   모델(.sql)만 바뀌면 이 레이어는 캐시 재사용 → 매번 deps 재설치 안 함.
#   deps 실패 시 빌드도 실패(|| true 제거) → dbt_utils 누락된 이미지가 조용히 나가는 것 방지.
COPY packages.yml dbt_project.yml ./
RUN dbt deps

# 나머지 프로젝트(models·seeds·macros 등).
#   주의: 이 COPY로 프로젝트 루트에도 profiles.yml이 딸려오지만,
#   런타임에 dbt가 쓰는 건 /root/.dbt/profiles.yml(Cosmos ProfileConfig가 지정)뿐이다.
COPY . .
