import pandas as pd
from tqdm import tqdm

# 파일 경로 매핑 (입력 파일과 해상도별 계산된 파일 연결)
input_files = {
    "F:\\INKLE\\2024_01_10\\daily_parquets\\daily_parquets\\merged_20250108_277x306.parquet": "F:\\INKLE\\2024_01_10\\precomputed_coordinates_res_2.0.parquet",
    "F:\\INKLE\\2024_01_10\\daily_parquets\\daily_parquets\\merged_20250108_555x612.parquet": "F:\\INKLE\\2024_01_10\\precomputed_coordinates_res_1.0.parquet",
    "F:\\INKLE\\2024_01_10\\daily_parquets\\daily_parquets\\merged_20250108_1110x1225.parquet": "F:\\INKLE\\2024_01_10\\precomputed_coordinates_res_0.5.parquet",
}

# 좌표 변환 함수
def replace_coordinates(input_parquet_path, precomputed_parquet_path, output_parquet_path):
    """
    기존 parquet 파일에서 x, y 좌표를 미리 계산된 Latitude, Longitude로 대체하는 함수.

    Parameters:
    - input_parquet_path: 기존 데이터 parquet 파일 경로
    - precomputed_parquet_path: 미리 계산된 좌표 데이터 parquet 파일 경로
    - output_parquet_path: 변환된 데이터를 저장할 parquet 파일 경로
    """
    # 기존 데이터 읽기
    data = pd.read_parquet(input_parquet_path)

    # 미리 계산된 좌표 데이터 읽기
    precomputed_data = pd.read_parquet(precomputed_parquet_path)
    precomputed_data.set_index(['x', 'y'], inplace=True)  # x, y를 인덱스로 설정

    # tqdm을 사용하여 좌표 변환
    tqdm.pandas(desc=f"Processing {input_parquet_path}")
    def map_coordinates(row):
        key = (row['x'], row['y'])
        if key in precomputed_data.index:
            lat, lon = precomputed_data.loc[key, ['Latitude', 'Longitude']]
            return pd.Series([lat, lon])
        else:
            return pd.Series([None, None])  # 변환되지 않은 좌표는 NaN으로 처리

    data[['Latitude', 'Longitude']] = data.progress_apply(map_coordinates, axis=1)

    # 기존 x, y 컬럼 삭제
    data.drop(columns=['x', 'y'], inplace=True)

    # 변환된 데이터 저장
    data.to_parquet(output_parquet_path, index=False)
    print(f"변환된 데이터가 저장되었습니다: {output_parquet_path}")

if __name__ == "__main__":
    for input_path, precomputed_path in input_files.items():
        # 출력 파일 경로 설정
        output_path = input_path.replace(".parquet", "_converted.parquet")

        # 변환 실행
        replace_coordinates(input_path, precomputed_path, output_path)
