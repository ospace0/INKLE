import pandas as pd
from pyproj import Proj, Transformer
from multiprocessing import Pool, cpu_count
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
            "x_start": 1430, "x_end": 2665,
            "y_start": 1545, "y_end": 2283,
            "image_width": 3600,
            "image_height": 3600,
            "x_min": -899750,
            "x_max": 899750,
            "y_min": -899750,
            "y_max": 899750,
        }
    elif resolution == 1.0:
        return {
            "x_start": 715, "x_end": 1327,
            "y_start": 772, "y_end": 1441,
            "image_width": 1800,
            "image_height": 1800,
            "x_min": -899500,
            "x_max": 899500,
            "y_min": -899500,
            "y_max": 899500,
        }
    elif resolution == 2.0:
        return {
            "x_start": 357, "x_end": 443,
            "y_start": 410, "y_end": 720,
            "image_width": 900,
            "image_height": 900,
            "x_min": -899000,
            "x_max": 899000,
            "y_min": -899000,
            "y_max": 899000,
        }

def pixel_to_latlon(args):
    pixel_x, pixel_y, params, lcc_params = args
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
    x = params["x_min"] + (pixel_x / (params["image_width"] - 1)) * (params["x_max"] - params["x_min"])
    y = params["y_max"] - (pixel_y / (params["image_height"] - 1)) * (params["y_max"] - params["y_min"])
    lon, lat = transformer.transform(x, y)
    return pixel_x, pixel_y, lat, lon

def latlon_to_pixel(lat, lon, resolution):
    """
    주어진 위도와 경도에서 픽셀 좌표 (x, y)를 계산합니다.

    Parameters:
    - lat: 위도 (Latitude)
    - lon: 경도 (Longitude)
    - resolution: 해상도 (0.5, 1.0, 2.0 중 하나)

    Returns:
    - x: 픽셀 x 좌표
    - y: 픽셀 y 좌표
    """
    params = get_lcc_params_by_resolution(resolution)
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
    transformer = Transformer.from_proj(wgs_proj, lcc_proj)

    # 위도/경도를 Lambert 좌표로 변환
    x_m, y_m = transformer.transform(lon, lat)

    # Lambert 좌표를 픽셀 좌표로 변환
    x = int((x_m - params["x_min"]) / (params["x_max"] - params["x_min"]) * (params["image_width"] - 1))
    y = int((params["y_max"] - y_m) / (params["y_max"] - params["y_min"]) * (params["image_height"] - 1))

    return x, y

if __name__ == '__main__':
    # 테스트 좌표
    locations = [
        {"name": "LT", "lat": 39, "lon": 124},
        {"name": "LB", "lat": 33, "lon": 124},
        {"name": "RT", "lat": 39, "lon": 132},
        {"name": "RB", "lat": 33, "lon": 132},
    ]

    resolutions = [0.5, 1.0, 2.0]

    # 모든 좌표에 대해 모든 해상도 계산
    for location in locations:
        print(f"\n{location['name']}:")
        for resolution in resolutions:
            x, y = latlon_to_pixel(location["lat"], location["lon"], resolution)
            print(f"  Resolution {resolution}: x = {x}, y = {y}")
