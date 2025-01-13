import os
import pandas as pd
from glob import glob

# 디렉토리 경로 설정 (CSV 파일들이 위치한 폴더)
input_directory = "D:/sat_file"
output_directory = "D:/mer"

if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# CSV 파일 검색
csv_files = glob(os.path.join(input_directory, "*.csv"))

# 파일 정보 추출 및 그룹화
grouped_files = {}
for file_path in csv_files:
    file_name = os.path.basename(file_path)
    parts = file_name.split("_")
    
    # 날짜, 시간, 배열 크기 추출
    date = parts[3]
    time = parts[4]
    size = parts[5].replace(".csv", "")
    
    # 그룹 키 생성 (날짜 + 배열 크기)
    group_key = f"{date}_{size}"
    
    if group_key not in grouped_files:
        grouped_files[group_key] = []
    grouped_files[group_key].append((time, file_path))

# 그룹별 파일 병합
for group_key, files in grouped_files.items():
    files.sort()  # 시간 순서대로 정렬
    
    merged_data = []
    for time, file_path in files:
        df = pd.read_csv(file_path)
        
        # 시간 컬럼 추가 (yyyymmddhh)
        hour = time.zfill(2)  # 시간 형식을 두 자리로 맞춤
        datetime_str = group_key.split("_")[0] + hour
        df.insert(0, "Datetime", datetime_str)
        
        merged_data.append(df)
    
    # 병합된 데이터 저장
    merged_df = pd.concat(merged_data, ignore_index=True)
    output_file = os.path.join(output_directory, f"merged_{group_key}.csv")
    merged_df.to_csv(output_file, index=False)

print("모든 파일이 병합되었습니다!")
