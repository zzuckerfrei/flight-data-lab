"""Slack 실패 알림 (#33, FD-W305) — 다층 방어선의 '마지막 층'.

설계:
  - Airflow의 on_failure_callback(task 최종 실패 시 자동 호출되는 훅)에 연결.
  - 전송은 SlackWebhookHook(conn_id=slack_default) → Incoming Webhook.
    Connection은 helm extraEnv의 AIRFLOW_CONN_SLACK_DEFAULT(k8s Secret slack-webhook)로 주입.
  - "실패 + 게이트 구분": 실패 task_id를 검사해 다층 방어선 게이트 차단과 일반 실패를 나눈다.

게이트 판정(실측한 task_id 기준):
  - bronze_opensky_states_bronze.source → freshness(최신성) 게이트
  - bronze_opensky_states_bronze.test   → volume/완결성 게이트
  둘 다 bronze source에 걸려 있으므로 prefix 'bronze_opensky_states_bronze'로 판정한다.
  그 외(stg/mart/seed의 dbt test, 수집·적재 task)는 일반 파이프라인 실패로 본다.

주의: 콜백에서 던진 예외가 다른 부작용을 만들지 않도록 전체를 방어(try/except)한다.
      알림 전송 실패는 삼키되 task 로그에는 남긴다(알림 유실을 조용히 넘기지 않기).
"""
import logging

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

log = logging.getLogger(__name__)

SLACK_CONN_ID = "slack_default"

# 다층 방어선 게이트 task의 task_id 접두어(freshness=.source / volume 완결성=.test).
GATE_TASK_PREFIX = "bronze_opensky_states_bronze"

# opensky_to_gcs의 Dynamic Task Mapping 인덱스 → 영역 이름(로그 가독성용, dags/opensky_to_gcs.py의 REGIONS 순서).
MAP_INDEX_TO_REGION = {0: "ukraine", 1: "middle_east", 2: "west_europe", 3: "korea"}


def notify_failure(context):
    """on_failure_callback 진입점. context에서 실패 정보를 뽑아 Slack blocks로 보낸다."""
    try:
        ti = context.get("task_instance")
        dag_id = getattr(ti, "dag_id", "?")
        task_id = getattr(ti, "task_id", "?")
        try_number = getattr(ti, "try_number", "?")
        map_index = getattr(ti, "map_index", -1)
        log_url = getattr(ti, "log_url", None)
        run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", "?")
        exc = context.get("exception")

        is_gate = task_id.startswith(GATE_TASK_PREFIX)
        if is_gate:
            gate_kind = "freshness(최신성)" if task_id.endswith(".source") else "volume(완결성)"
            title = "🚨 데이터 품질 게이트 차단"
            summary = (
                f"다층 방어선 *{gate_kind}* 게이트 실패 → 하류 dbt 변환 차단됨. "
                f"bronze 데이터의 신선도/완결성 확인 필요."
            )
        else:
            title = "❌ 파이프라인 실패"
            summary = "task 실행이 최종 실패했습니다."

        # 매핑 task면 영역 표기(예: map_index=0 → ukraine)
        task_label = task_id
        if map_index is not None and map_index >= 0:
            region = MAP_INDEX_TO_REGION.get(map_index, f"map_index={map_index}")
            task_label = f"{task_id} [{region}]"

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": title, "emoji": True}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:*\n`{task_label}`"},
                    {"type": "mrkdwn", "text": f"*Run:*\n`{run_id}`"},
                    {"type": "mrkdwn", "text": f"*Try:*\n{try_number}"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
        ]

        # 예외 메시지(있으면, 과도한 길이는 잘라 첨부)
        if exc:
            exc_text = str(exc)
            if len(exc_text) > 800:
                exc_text = exc_text[:800] + " …(생략)"
            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Error:*\n```{exc_text}```"}}
            )

        # 로그 바로가기(BASE_URL=localhost:8080 기준 브라우저 링크)
        if log_url:
            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": f"<{log_url}|📜 로그 보기>"}}
            )

        # text는 알림/미리보기용 fallback(blocks 미지원 클라이언트 대비)
        fallback = f"{title} — {dag_id}.{task_label} (run={run_id})"
        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send(text=fallback, blocks=blocks)
        log.info("Slack 실패 알림 전송: %s.%s", dag_id, task_id)

    except Exception:  # 콜백 예외가 다른 부작용을 만들지 않도록 방어. 단, 로그엔 남긴다.
        log.exception("Slack 실패 알림 전송 실패(콜백 내부 예외)")
