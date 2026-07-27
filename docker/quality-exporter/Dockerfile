# 데이터 품질 메트릭 exporter 이미지 (#50, 층 B).
# BQ 인증(SA키)은 이미지에 굽지 않고 런타임 k8s Secret 볼륨으로 주입(dbt·kepler와 동일 패턴).
FROM python:3.12-slim

WORKDIR /app

# 의존성 먼저(레이어 캐시): 코드만 바뀌면 재설치 안 함.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py .

EXPOSE 8000
CMD ["python", "-u", "exporter.py"]
