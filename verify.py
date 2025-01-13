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
    # Lambert Conformal Conic (LCC) 투영 설정
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
    # WGS84(위경도 좌표계)
    wgs_proj = Proj(proj="latlong", datum="WGS84")
    # Transformer 생성 (LCC -> WGS84)
    transformer = Transformer.from_proj(lcc_proj, wgs_proj)
    # 픽셀 위치 -> LCC 투영 좌표 변환
    x = x_min + (pixel_x / (image_width - 1)) * (x_max - x_min)
    y = y_max - (pixel_y / (image_height - 1)) * (y_max - y_min)
    # LCC -> 위경도 변환
    lon, lat = transformer.transform(x, y)
    return lat, lon

# x, y -> lat, lon 변환 캐싱 딕셔너리
coordinate_cache = {}

# Parquet 파일 처리 함수
def process_parquet(input_parquet_path, output_parquet_path, resolution):
    params = get_lcc_params_by_resolution(resolution)
    data = pd.read_parquet(input_parquet_path, engine = 'fastparquet')  # Parquet 파일 읽기
    latitudes = []
    longitudes = []

    # x, y 값을 lat, lon으로 변환 (tqdm으로 진행률 표시)
    for index, row in tqdm(data.iterrows(), total=len(data), desc="변환 진행률", unit="건"):
        x = row['x'] + params["x_offset"]
        y = row['y'] + params["y_offset"]
        key = (x, y)  # 캐싱을 위한 키 생성
        if key in coordinate_cache:
            lat, lon = coordinate_cache[key]  # 캐싱된 값을 사용
        else:
            lat, lon = pixel_to_latlon(
                x, y,
                params["image_width"], params["image_height"],
                params["x_min"], params["x_max"],
                params["y_min"], params["y_max"],
                lcc_params
            )
            coordinate_cache[key] = (lat, lon)
        latitudes.append(lat)
        longitudes.append(lon)
    
    # 결과를 데이터프레임에 추가
    data['Latitude'] = latitudes
    data['Longitude'] = longitudes
    data.drop(columns=['x', 'y'], inplace=True)

    # 변환된 데이터를 Parquet 파일로 저장
    data.to_parquet(output_parquet_path, index=False)
    print(f"변환된 Parquet 파일이 {output_parquet_path}에 저장되었습니다.")

# 파일 경로 설정
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
input_parquet_path = r"F:\INKLE\2024_01_10\daily_parquets\daily_parquets\merged_20250108_277x306.parquet"
output_parquet_path = f"F:\INKLE\2024_01_10\daily_parquets\daily_parquets\merged_20250108_277x306_converted.parquet"
resolution = 2.0  # 해상도 설정

# 함수 실행
process_parquet(input_parquet_path, output_parquet_path, resolution)
