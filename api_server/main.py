# 0. 요구사항 정의, 모듈 설명
'''
MSA구조상 신용평가 업무만 담당하는 API

추론 요청(데이터 포함)-> 추론 -> 결과 돌려줌
요청시 데이터
    - 1명 ~ (*)여러명 요청 -> [ 개별정보(Pydantic 정의), 개별정보, ..]
응답 데이터 
    - 1명 ~ (*)여러명 응답 -> [ 평가정보(Pydantic 정의), 평가정보, ...]
신용 점수 평가 모델은 -> 더미 (간단한 공식(수식)으로 진행) 
            -> 향후 실제모델로 교체하면됨(모델 엔트리포인트(API) 호출)
'''
# 1. 모듈 가져오기
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import random

# 2. 앱 생성
app = FastAPI()

# 3. 요청/응답시 전달되는(하는) 데이터 형태 + 유효성 자동 검사 =>pydantic
class ReqData(BaseModel): # 요청
    # 사용자 아이디, 소득, 대출총량
    user_id: str
    income: int
    loan_amt: int
    pass
class ResData(BaseModel): # 응답
    # 사용자 아이디, 신용점수, 등급
    user_id: str
    credit_score: int
    grade: str
    pass

# 4. 라우팅
## 홈 -> 헬시체크(서비스가 살아 있는지 주기적 요청)용도로 활용 가능
@app.get('/')
def home():
    return {'status':"AI Predict Server is Running"}

## 신용예측
@app.post("/predict", response_model=List[ResData])
def predict(users: List[ReqData]):
    '''
    AI 모델(더미)을 이용한 신용평가 서비스    
    :param uses: n명의 사용자의 정보(아이디, 소득, 대출양)
    :type uses: List[ReqData]
    '''
    # AI 모델이 없으므로 가상의 계산식으로 평가
    # 식1 = (소득 // 1000) * 10
    # credit_score = 식2 = min( 난수값(300, 600) + 식1, 990 )
    # grade = 식2 > 800 크면 "A"등급, > 600 크면 "B"등급, 나머지는 "C"등급
    # 결과물 : [ {user_id:xx, credit_score:xx, grade:xx }, {}, .... ]
    # 위의 요구사항대로 구현하시오
    results = list()
    for user in users:
        # 더미 계산
        식1 = (user.income // 1000) * 10
        credit_score = min( random.randint(300, 600) + 식1, 990 )
        grade = "A" if credit_score>800 else "B" if credit_score>600 else "C"
        # 결과 담기
        results.append({
            "user_id":user.user_id, 
            "credit_score":credit_score, 
            "grade":grade
        })
    return results