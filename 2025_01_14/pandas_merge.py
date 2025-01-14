import pandas as pd

# 파일 템플릿
input_template = r"F:\INKLE\2025_01_13\merged_20250110_{resolution}.parquet"
coordinates_template = r"F:\INKLE\2024_01_10\precomputed_coordinates_res_{resolution}.parquet"
output_template = r"final_combined_with_coordinates_{resolution}.parquet"

# 해상도 리스트
resolutions = ["0.5", "1.0", "2.0"]

try:
    for resolution in resolutions:
        print(f"Processing resolution: {resolution}")

        # 각 해상도에 해당하는 파일 경로 설정
        input_file = input_template.format(resolution=resolution)
        coordinates_file = coordinates_template.format(resolution=resolution)
        output_file = output_template.format(resolution=resolution)

        # 데이터 읽기
        combined_df = pd.read_parquet(input_file)
        coordinates_df = pd.read_parquet(coordinates_file)

        # 좌표 데이터에서 Latitude, Longitude 열만 추출
        coordinates_df = coordinates_df[["Latitude", "Longitude"]]

        # 데이터 길이 일치
        min_rows = min(len(combined_df), len(coordinates_df))
        combined_df = combined_df.iloc[:min_rows]
        coordinates_df = coordinates_df.iloc[:min_rows]

        # 기존 데이터의 x, y를 Latitude, Longitude로 교체
        combined_df["Latitude"] = coordinates_df["Latitude"]
        combined_df["Longitude"] = coordinates_df["Longitude"]

        # 기존 x, y 열 제거
        combined_df = combined_df.drop(columns=["x", "y"], errors="ignore")

        
       # 데이터 타입 최적화
        print("  Optimizing data types...")
        for col in combined_df.columns:
            if combined_df[col].dtype == "float64":
                combined_df[col] = combined_df[col].astype("float16")
            elif combined_df[col].dtype == "int64":
                combined_df[col] = combined_df[col].astype("int32")
        
        # 데이터를 24배 반복
        print("  Repeating data 24 times...")
        repeated_combined_df = pd.concat([combined_df] * 24, ignore_index=True)

        # 최종 데이터를 Parquet 파일로 저장
        print(f"  Saving final data to {output_file}...")
        repeated_combined_df.to_parquet(output_file, index=False)
        print(f"  Final combined data saved to {output_file}")

except Exception as e:
    print(f"Error processing files: {e}")
