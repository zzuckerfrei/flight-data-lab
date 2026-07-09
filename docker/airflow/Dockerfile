# 커스텀 Airflow 이미지: base(apache/airflow:3.2.2)에 provider를 "구워" _PIP_ADDITIONAL_REQUIREMENTS 제거.
# 근거(표준): _PIP는 프로덕션 금지(재시작 지연 + leftpad 취약점). 공식 Helm Production Guide = 커스텀 이미지로 확장.
#   - astronomer-cosmos: dbt를 Airflow DAG/TaskGroup으로 오케스트레이션 (#27, K8s 실행 모드)
#   - apache-airflow-providers-keycloak: 기존 _PIP로 설치하던 SSO auth manager provider를 이미지로 이전
# cncf.kubernetes provider는 base 이미지에 이미 포함(10.17.1, KubernetesExecutor)이라 추가/고정하지 않음(다운그레이드 방지).
FROM apache/airflow:3.2.2

# 버전 고정 = 재현성 + 공급망 방어. base와 동일한 Airflow 3.2.2 / Python 3.13 위에 얹는다.
RUN pip install --no-cache-dir \
      "astronomer-cosmos==1.15.0" \
      "apache-airflow-providers-keycloak==0.8.1"
