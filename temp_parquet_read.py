import pandas as pd

# Parquet 파일 경로 설정
file_path = r"F:\INKLE\2024_01_10\daily_parquets\temp_parquet_hourly\satellite_data_1havg_20250108_00_277x306_converted_20250110_125723.parquet"

# Parquet 파일 읽기
df = pd.read_parquet(file_path, engine='fastparquet')  # 또는 engine='fastparquet'

# 데이터 확인
print(df.head())  # 데이터프레임의 처음 5행 출력
print(f"총 행 개수: {len(df)}")
print(f"컬럼 정보: {df.columns}")
