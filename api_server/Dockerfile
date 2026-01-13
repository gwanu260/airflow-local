FROM python:3.12.12-slim

# 작업 디렉토리 지정(루트가됨)
WORKDIR /app

# 필요한 패키지 설치
RUN pip install fastapi uvicorn pydantic

# 소스 코드 복사
COPY main.py .

# 서버 실행
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# 필요 패키지 설치
RUN pip install fastapi uvicorn pydantic

# 소스코드 복사
COPY main.py .

# 서버 실행
CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" ]
