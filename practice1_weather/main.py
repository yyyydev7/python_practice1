"""
================================================================================
[프로그램 머리말 (Header)]
================================================================================
- 작성일자: 2026-07-15
- 작성자: 손연우
- 파일명: main.py
- 개발 목적: 공공 API를 활용한 실시간 날씨 데이터 비동기 수집 및 다양한 포맷 저장 실습
- 주요 기능:
  1. Open-Meteo API 및 TimeAPI를 통한 다국적 도시의 기온/현지시각 비동기 병렬 수집
  2. Pydantic 라이브러리를 활용한 데이터 스키마(Weather) 정의 및 타입 검증(Validation)
  3. Pandas를 활용하여 수집한 데이터를 CSV 및 Parquet 포맷으로 디스크에 안전하게 저장
  4. 예외 처리 패턴을 적용하여 파일 미존재 및 입출력 오류 상황을 견고하게 제어
  5. 파일 내부에서 pytest 데코레이터를 지정하여 서울 기온 검증 테스트 연동 준비
================================================================================
"""

import asyncio
import os
import sys
from typing import List, Union
import httpx
import pandas as pd
from pydantic import BaseModel, Field, ValidationError
import pytest


# ==============================================================================
# 2. Weather Pydantic 스키마 정의 (도시: str, 기온: float | str, 현지시각: str)
# ==============================================================================
class Weather(BaseModel):
    """도시별 날씨와 현지 시각 정보를 담는 데이터 모델
    도시 (str): 데이터 수집 대상 도시의 이름 (필수값)
    기온 (Union[float, str]): 현재 섭씨 기온. 수집 실패 시 문자열 "N/A" 처리 허용 (필수값)
    현지시각 (str): 해당 도시의 현지 날짜 및 시각 정보 (필수값)
    """

    # Field(..., ...)의 첫 번째 인자 '...'은 기본값이 없는 필수 입력 항목임을 의미합니다.
    도시: str = Field(..., description="도시 이름")
    기온: Union[float, str] = Field(..., description="현재 기온 (섭씨)")
    현지시각: str = Field(..., description="현지 날짜 및 시각 (MM/DD/YYYY HH:MM)")


# ==============================================================================
# 1. 공공 API 비동기 데이터 수집
# ==============================================================================
CITIES = [
    {"name": "서울", "lat": 37.5665, "lon": 126.9780, "tz": "Asia/Seoul"},
    {"name": "도쿄", "lat": 35.6762, "lon": 139.6503, "tz": "Asia/Tokyo"},
    {"name": "뉴욕", "lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    {"name": "런던", "lat": 51.5074, "lon": -0.1278, "tz": "Europe/London"},
]


async def fetch_city_data(
    client: httpx.AsyncClient, city: dict
) -> Union[Weather, None]:
    """특정 도시의 기온과 현지 시각 정보를 비동기로 수집하고 Weather 객체로 반환합니다.
    Args:
        client (httpx.AsyncClient): 커넥션 풀 재사용을 위한 비동기 HTTP 클라이언트 객체
        city (dict): 수집 대상 도시 정보 (이름, 위도, 경도, 시간대)

    Returns:
        Union[Weather, None]: 유효성 검증을 마친 Weather 인스턴스, 통신/검증 실패 시 None 반환"""
    # 1-1. 각 도시 정보 기반의 API 요청 URL 동적 구성
    weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
    time_url = f"https://timeapi.io/api/time/current/zone?timeZone={city['tz']}"

    try:
        # 비동기 병렬 호출
        # 1-2. 두 개의 외부 API 요청을 동시에 병렬(Concurrent) 처리하여 네트워크 대기 시간 최소화
        weather_resp, time_resp = await asyncio.gather(
            client.get(weather_url, timeout=5.0), client.get(time_url, timeout=5.0)
        )
        # HTTP 응답 상태 코드 검증 (200 OK가 아닐 시 HTTPStatusError 발생 유도)
        weather_resp.raise_for_status()
        time_resp.raise_for_status()

        weather_data = weather_resp.json()
        time_data = time_resp.json()

        # 1-3. 기온 정보 안전하게 추출 (API 스키마 예외 대비 기본값 설정)
        temp = weather_data.get("current_weather", {}).get("temperature", "N/A")

        # 1-4. 시간 포맷팅 (MM/DD/YYYY HH:MM)
        raw_time = time_data.get("dateTime", "")
        try:
            formatted_time = pd.to_datetime(raw_time).strftime("%m/%d/%Y %H:%M")
        except Exception:
            formatted_time = (
                raw_time if raw_time else "N/A"
            )  # 파싱 오류 시 원본 데이터 보존

        # Pydantic을 이용한 런타임 데이터 검증 및 객체 생성 리턴
        return Weather(도시=city["name"], 기온=temp, 현지시각=formatted_time)

    except (httpx.HTTPStatusError, httpx.RequestError, ValidationError) as e:
        # 통신 장애, 에러 응답 및 데이터 타입 불일치 예외를 안전하게 가로채고 로그 출력
        print(f"[수집 오류] {city['name']} 데이터 처리 실패: {e}")
        return None


async def collect_all_weather() -> List[Weather]:
    """
    모든 도시에 대해 비동기 방식으로 데이터를 수집합니다.
    Returns:
        List[Weather]: 정상적으로 수집 및 데이터 검증이 완료된 도시별 Weather 객체 목록
    """
    async with httpx.AsyncClient() as client:
        # 각 도시별 수집 함수를 코루틴 태스크 리스트로 변환
        tasks = [fetch_city_data(client, city) for city in CITIES]
        # 모든 태스크를 동시에 실행하고 대기
        results = await asyncio.gather(*tasks)
        # 통신 에러 등으로 수집에 실패하여 'None'이 된 요소는 리스트에서 제외하고 필터링
        return [res for res in results if res is not None]


# ==============================================================================
# 3. Weather 객체를 CSV로 저장하고 읽어오기 (해당 파일이 없으면 예외처리)
# ==============================================================================
def save_and_read_csv(weather_list: List[Weather], filename: str = "weather.csv"):
    """
    Weather 데이터를 CSV로 저장한 후, 유효성을 검사하며 다시 읽어옵니다.
    Args:
        weather_list (List[Weather]): 저장 대상 Weather 객체 리스트
        filename (str): 내보낼 CSV 파일 경로 (기본값: weather.csv)

    Raises:
        FileNotFoundError: 작성 및 로드 과정에서 파일이 물리적으로 디스크에 존재하지 않을 경우 발생
    """
    # Pydantic 객체를 딕셔너리로 언팩한 뒤 DataFrame으로 빠르게 변환
    df = pd.DataFrame([w.model_dump() for w in weather_list])
    # 한글 깨짐 방지 인코딩(utf-8-sig)을 사용하여 CSV 디스크 저장 수행
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"[{filename}] 저장 완료.")

    # 예외 처리: 파일 미존재 시
    if not os.path.exists(filename):
        raise FileNotFoundError(f"에러: {filename} 파일이 존재하지 않습니다.")

    # 데이터가 정상적으로 저장되었는지 검증하기 위해 파일을 다시 읽어와 출력
    df_read = pd.read_csv(filename)
    print("\n[3. CSV 읽기 결과]")
    print(df_read)


# ==============================================================================
# 4. Weather 객체를 Parquet으로 저장하고 도시, 기온만 읽어오기 (해당 파일이 없으면 예외처리)
# ==============================================================================
def save_and_read_parquet(
    weather_list: List[Weather], filename: str = "weather.parquet"
):
    """
    Weather 데이터를 Parquet으로 저장한 후, 도시와 기온 컬럼만 필터링하여 다시 읽어옵니다.
    Args:
        weather_list (List[Weather]): 저장 대상 Weather 객체 리스트
        filename (str): 내보낼 Parquet 파일 경로 (기본값: weather.parquet)

    Raises:
        FileNotFoundError: 작성 및 로드 과정에서 파일이 물리적으로 디스크에 존재하지 않을 경우 발생
    """
    df = pd.DataFrame([w.model_dump() for w in weather_list])
    # pyarrow 엔진을 백엔드로 활용하여 Parquet 직렬화 진행
    df.to_parquet(filename, index=False, engine="pyarrow")
    print(f"\n[{filename}] 저장 완료.")

    # 예외 처리: 파일 미존재 시
    if not os.path.exists(filename):
        raise FileNotFoundError(f"에러: {filename} 파일이 존재하지 않습니다.")

    # '도시', '기온' 열만 부분 로드
    df_read = pd.read_parquet(filename, columns=["도시", "기온"])
    print("\n[4. Parquet 읽기 결과 (도시, 기온)]")
    print(df_read)


# ==============================================================================
# 5. pytest로 검사 (서울 기온이 25도가 아니면 Fail 메시지 출력)
# ==============================================================================
@pytest.mark.asyncio
async def test_seoul_temperature():
    """서울의 현재 기온이 정확히 25도가 아닌 경우 Fail 처리를 수행합니다."""
    city = {"name": "서울", "lat": 37.5665, "lon": 126.9780, "tz": "Asia/Seoul"}
    weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"

    async with httpx.AsyncClient() as client:
        response = await client.get(weather_url)
        assert response.status_code == 200

        data = response.json()
        temp = data.get("current_weather", {}).get("temperature")

        # 기온 단언 검사: 25.0도가 아닐 시 디버깅에 용이하도록 현재 기온 정보를 로깅하며 테스트 실패 처리
        assert temp == 25.0, f"현재 서울 기온은 {temp}°C 입니다. (기대값: 25.0°C)"


# ==============================================================================
# 실행 진입점 (1~4번 과정 수행)
# ==============================================================================
async def main():
    """
    전체 시스템 실행 시나리오를 동기적으로 제어하고 스케줄링하는 마스터 제어 함수.
    """
    print("===== [실습 1] 비동기 데이터 수집 시작 =====")
    weather_data = await collect_all_weather()

    print("\n===== [실습 3] CSV 저장 및 읽기 시작 =====")
    save_and_read_csv(weather_data)

    print("\n===== [실습 4] Parquet 저장 및 읽기 시작 =====")
    save_and_read_parquet(weather_data)


if __name__ == "__main__":
    # 윈도우 환경에서 비동기 입출력(asyncio) 실행 시 발생할 수 있는 소켓 오류 예방책 적용
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # 비동기 루프 엔진 구동 및 main 시나리오 전개
    asyncio.run(main())
