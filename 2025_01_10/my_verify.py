import pandas as pd
from datetime import datetime
from pyproj import Proj, Transformer
from tqdm import tqdm

# Lambert Conformal Conic (LCC) 투영 설정
lcc_params = {
    "lat_1": 30.0,   # 표준 평행선 1
    "lat_2": 60.0,   # 표준 평행선 2
    "lat_0": 38.0,   # 원점 위도
    "lon_0": 126.0   # 중심 자오선
}

def get_lcc_params_by_resolution(resolution):
    if resolution == 0.5:
        return {
            "image_width": 3600,
            "image_height": 3600,
            "x_min": -899750,
            "x_max": 899750,
            "y_min": -899750,
            "y_max": 899750,
            "x_offset": 1430,
            "y_offset": 1773
        }
    elif resolution == 1.0:
        return {
            "image_width": 1800,
            "image_height": 1800,
            "x_min": -899500,
            "x_max": 899500,
            "y_min": -899500,
            "y_max": 899500,
            "x_offset": 715,
            "y_offset": 886
        }
    elif resolution == 2.0:
        return {
            "image_width": 900,
            "image_height": 900,
            "x_min": -899000,
            "x_max": 899000,
            "y_min": -899000,
            "y_max": 899000,
            "x_offset": 357,
            "y_offset": 443
        }

def pixel_to_latlon(pixel_x, pixel_y, image_width, image_height, x_min, x_max, y_min, y_max, lcc_params):
    lcc_proj = Proj(
        proj="lcc",
        lat_1=lcc_params["lat_1"],  # 표준 평행선 1
        lat_2=lcc_params["lat_2"],  # 표준 평행선 2
        lat_0=lcc_params["lat_0"],  # 원점 위도
        lon_0=lcc_params["lon_0"],  # 중심 자오선
        x_0=0,
        y_0=0,
        ellps="WGS84"  # 타원체
    )
    wgs_proj = Proj(proj="latlong", datum="WGS84")
    transformer = Transformer.from_proj(lcc_proj, wgs_proj)
    x = x_min + (pixel_x / (image_width - 1)) * (x_max - x_min)
    y = y_max - (pixel_y / (image_height - 1)) * (y_max - y_min)
    lon, lat = transformer.transform(x, y)
    return lat, lon

def process_parquet(input_parquet_path, output_parquet_path, resolution):
    params = get_lcc_params_by_resolution(resolution)
    data = pd.read_parquet(input_parquet_path, engine='fastparquet')  # Parquet 파일 읽기

    # 첫 277x306 블록 변환
    rows_per_block = 277 * 306
    first_block = data.iloc[:rows_per_block]

    # 진행률 표시를 위한 tqdm 사용
    tqdm.pandas(desc="첫 블록 변환 진행")
    first_block['x'], first_block['y'] = zip(*first_block.progress_apply(
        lambda row: pixel_to_latlon(
            row['x'] + params["x_offset"],
            row['y'] + params["y_offset"],
            params["image_width"],
            params["image_height"],
            params["x_min"],
            params["x_max"],
            params["y_min"],
            params["y_max"],
            lcc_params
        ), axis=1))

    # 나머지 블록은 복사
    num_blocks = len(data) // rows_per_block
    final_data = pd.concat([first_block] * num_blocks, ignore_index=True)
    final_data = final_data[:len(data)]  # 데이터 크기를 맞춤

    # 변환된 데이터를 Parquet 파일로 저장
    final_data.to_parquet(output_parquet_path, index=False)
    print(f"변환된 Parquet 파일이 {output_parquet_path}에 저장되었습니다.")

# 파일 경로 설정
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
input_parquet_path = r"F:\INKLE\2024_01_10\daily_parquets\daily_parquets\merged_20250108_277x306.parquet"
output_parquet_path = r"F:\INKLE\2024_01_10\daily_parquets\daily_parquets\merged_20250108_277x306_converted.parquet"
resolution = 2.0  # 해상도 설정

# 함수 실행
process_parquet(input_parquet_path, output_parquet_path, resolution)
