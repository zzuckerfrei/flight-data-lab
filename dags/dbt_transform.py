"""
#27 5단계 — Cosmos로 dbt를 Airflow에서 실행 (Kubernetes 실행 모드)

구조의 핵심 = "파싱(계획)"과 "실행(dbt run)"이 서로 다른 파드에서 일어난다:
  - Airflow(dag-processor)가 manifest.json(우리 dbt 프로젝트 '지도')만 읽어 task 그래프를 그림.
    → Airflow 노드엔 dbt를 설치할 필요가 없다(LoadMode.DBT_MANIFEST).
  - 실제 dbt run은 dbt-flight 이미지로 띄운 전용 Pod(KubernetesPodOperator)에서 실행.

★ 그래서 경로 값이 "어느 파드 기준이냐"로 나뉜다(가장 헷갈리기 쉬운 지점):
  - manifest_path                      → Airflow 파드 기준 (이 DAG 파일 옆 dbt_manifest/)
  - profiles_yml_filepath, dbt_project_path → dbt 실행 Pod(dbt-flight) 안 기준
"""
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, SourceRenderingBehavior
from airflow.providers.cncf.kubernetes.secret import Secret

from utils.slack_alerts import notify_failure  # 실패 시 Slack 알림 (#33)

# ── 부품1: ProjectConfig — 지도 위치 + 프로젝트명 (파싱은 Airflow가 하므로 Airflow 경로) ──
# manifest_path는 절대경로 대신 __file__ 상대경로로 잡는다.
#   이유: git-sync는 새 커밋마다 .worktrees/<커밋해시>/ 라는 새 폴더에 repo를 풀어서
#   절대경로(/opt/airflow/dags/repo/...)가 커밋마다 바뀐다. "이 파일 옆 dbt_manifest/"로
#   상대 지정하면 폴더명이 바뀌어도 항상 맞다.
# DBT_MANIFEST 모드라 dbt_project_path(프로젝트 통째) 대신 지도(manifest)+이름만 준다.
project_config = ProjectConfig(
    manifest_path=Path(__file__).parent / "dbt_manifest" / "manifest.json",
    project_name="flight_data",
)

# ── 부품2: ProfileConfig — 인증 (실제 dbt run이 쓰므로 dbt Pod 경로) ──
# 이미 dbt-flight 이미지에 profiles.yml을 구워뒀으므로 profile_mapping(Airflow Connection에서
# 자동생성)이 아니라 파일 경로 직접 지정 방식을 쓴다.
# target=prod = SA키 인증(자동화용). dev=OAuth(내 계정)와 같은 BigQuery에 붙되 인증 방식만 다름.
# SA키 파일 자체는 이미지에 굽지 않고 런타임 Secret으로 주입한다(아래 operator_args).
profile_config = ProfileConfig(
    profile_name="flight_data",                       # profiles.yml 최상위 키(dbt_project명과 일치)
    target_name="prod",                               # 운영: SA키 인증 + staging/marts 데이터셋(접두어 없음). k8s_dev로 전체 검증 완료 후 전환(2026-07-18).
    profiles_yml_filepath="/root/.dbt/profiles.yml",  # ← dbt Pod 안 경로 (Airflow 파드 아님!)
)

# ── 부품3: ExecutionConfig — 어떻게 실행 (KUBERNETES = dbt 전용 Pod로 격리) ──
# dbt_project_path는 dbt Pod 안 프로젝트 경로(이미지 WORKDIR=/usr/app/dbt).
# K8s 실행 모드에선 이 값을 ProjectConfig가 아니라 ExecutionConfig에 둔다(둘은 상호배타).
#   이유: 이 경로는 '실행 Pod 안' 경로라서 실행 설정에 속한다.
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.KUBERNETES,
    dbt_project_path="/usr/app/dbt",                  # ← dbt Pod 안 경로
)

# ── 부품4: RenderConfig — 어떻게 파싱 (DBT_MANIFEST = 지도만 읽음) ──
# Astronomer 공식이 K8s 실행에 권장하는 조합: manifest로 파싱하면 Airflow에 dbt 불필요 + 가장 빠름.
# source_rendering_behavior=with_tests_or_freshness (Cosmos 1.6+): source 노드 중
#   freshness나 test가 달린 것을 Airflow task로 렌더 → 다층 방어 게이트를 dbt run '앞단'에 배치.
#   dbt 의존성상 source가 최상류라, source freshness/test가 자동으로 staging·marts보다 앞에 온다.
#   (freshness=최신성 게이트 / bronze 완결성 singular test=완결성 게이트 → 통과해야 dbt run 진행)
render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
)

# ── operator_args: 각 dbt 모델을 실행할 KubernetesPodOperator Pod 공통 스펙 ──
# Cosmos의 K8s 오퍼레이터는 KubernetesPodOperator를 상속하므로 아래 값들이 그대로 Pod 스펙이 된다.
# secrets = "받는 주소(profiles.yml) vs 배달(Secret)"의 '배달' 쪽:
#   gcp-sa-key Secret을 Pod의 /root/.dbt/gcp-sa-key.json 파일로 꽂는다.
#   → profiles.yml prod의 keyfile이 그 경로를 읽어 BQ 인증(주소는 profiles, 실제 키는 여기 Secret).
operator_args = {
    "image": "dbt-flight:1.0.0",
    "image_pull_policy": "IfNotPresent",  # kind load한 로컬 이미지 → 원격 pull 금지(ImagePullBackOff 방지)
    "namespace": "airflow",               # gcp-sa-key Secret이 있는 ns(=dbt Pod도 여기 떠야 접근)
    # 성공 Pod는 삭제(dbt task 10개 × 매일 = 누적), 실패 Pod만 부검용 보존.
    # 애플리케이션 로그는 #43 GCS remote logging에 남으므로 성공 Pod를 남길 이유가 없다.
    # 단 Pod가 못 뜨는 실패(이미지 pull·OOMKilled·스케줄 불가)는 stdout이 없어 GCS에도 안 남음
    # → 그 경우만 kubectl describe가 필요하므로 delete_pod 대신 실패 Pod는 남긴다.
    "on_finish_action": "delete_succeeded_pod",
    "secrets": [
        Secret(
            deploy_type="volume",         # SA키는 파일이라 volume(env 아님)
            deploy_target="/root/gcp",    # ★ /root/.dbt와 분리: 같은 곳에 마운트하면 볼륨이 profiles.yml을 가림
            secret="gcp-sa-key",          # 기존 k8s Secret 이름(airflow ns)
            key="key.json",               # ★ volume 마운트에선 key= 무시되고 Secret의 실제 키 이름으로
                                          #   파일 생성됨 → 이 Secret 키가 'key.json'이라 /root/gcp/key.json.
                                          #   그래서 profiles.yml keyfile도 /root/gcp/key.json으로 맞춤.
        ),
    ],
}

# ── 조립: DbtDag(4부품 + operator_args) ──
# schedule="0 1 * * *": 하루 1회, UTC 01:00(=KST 10:00). 적재(gcs_to_bq)가 UTC 00:00에 도니 그 1시간 뒤.
#   왜 하루 1회 시간 오프셋인가 → ADR-0001(private/adr): 적재가 @daily 규칙적 + freshness 게이트가
#   '적재 실패 시 중단'을 이미 담당하므로 Asset(이벤트) 트리거는 이점 상쇄 → 단순한 시간 오프셋 채택.
#   full-refresh라 하루 2회는 헛수고(bronze가 하루 1번 갱신되니 변환도 1번이면 충분).
# catchup=False: 과거 변환 소급 불필요(marts는 최신 bronze 전량 재생성이라 과거 run 의미 없음).
dbt_transform_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    operator_args=operator_args,
    dag_id="dbt_transform",
    schedule="0 1 * * *",
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=["dbt", "cosmos", "marts"],
    # default_args → Cosmos가 생성하는 모든 dbt task(KubernetesPodOperator)에 상속.
    #   게이트 task(bronze_opensky_states_bronze.source/.test) 실패 시 게이트 차단 알림으로 구분됨.
    default_args={"on_failure_callback": notify_failure},
)
