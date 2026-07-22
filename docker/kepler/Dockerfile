# Kepler 지도 자동배포 전용 이미지 (#45). KubernetesPodOperator로 이 이미지 Pod가 떠서
#   BQ marts → keplergl HTML → GCS 업로드를 자족적으로 실행한다(dbt-flight 패턴과 동일).
# 담는 것: keplergl + pandas + google-cloud(BQ/Storage) + build_and_deploy.py.
# 담지 않는 것: SA 키(런타임에 k8s Secret으로 볼륨 주입, GOOGLE_APPLICATION_CREDENTIALS).
#
# 빌드:     docker build -t kepler-map:1.0.0 -f Dockerfile .
# kind 주입: kind load docker-image kepler-map:1.0.0 --name flight-data
FROM python:3.12-slim

# 의존성 버전 고정(재현성). setuptools<81 = keplergl 0.3.7이 쓰는 pkg_resources 제거 문제 대비(2026-07-21 실측).
# db-dtypes/pyarrow = bigquery.Client.query().to_dataframe()에 필요.
RUN pip install --no-cache-dir \
    "keplergl==0.3.7" \
    pandas \
    "google-cloud-bigquery" \
    "google-cloud-storage" \
    db-dtypes \
    pyarrow \
    "setuptools<81"

WORKDIR /app
COPY build_and_deploy.py /app/build_and_deploy.py
