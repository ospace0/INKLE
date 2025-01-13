import pandas as pd
from pyproj import Proj, transform
from tqdm import tqdm  # tqdm을 이용한 진행률 표시

# Parquet 파일 읽기
file_path = r"F:\INKLE\2024_01_10\daily_parquets\daily_parquets\merged_20250108_277x306.parquet"
df = pd.read_parquet(file_path, engine='fastparquet')  # 또는 engine='pyarrow'

# Lambert Conformal Conic (LCC) 도법 설정
proj_lcc = Proj(
    proj='lcc',
    lat_1=30,  # Standard Parallel1 [deg]
    lat_2=60,  # Standard Parallel2 [deg]
    lat_0=38,  # 원점 위도 [deg]
    lon_0=126,  # 중심 자오선 [deg]
    x_0=0,  # 기준 Easting
    y_0=0,  # 기준 Northing
    ellps='WGS84'  # WGS84 타원체 사용
)

# WGS84 위도/경도 좌표계
proj_latlon = Proj(proj='latlong', ellps='WGS84')

# 캐시 생성
coordinate_cache = {}

# x, y 좌표를 위도와 경도로 변환
def convert_to_lat_lon(row):
    x, y = row['x'], row['y']
    if (x, y) in coordinate_cache:  # 캐시에서 값을 찾음
        return pd.Series(coordinate_cache[(x, y)])
    else:  # 새 값을 계산하여 캐시에 저장
        lon, lat = transform(proj_lcc, proj_latlon, x, y, always_xy=True)
        coordinate_cache[(x, y)] = (lat, lon)
        return pd.Series([lat, lon])

# tqdm을 사용한 변환 적용
tqdm.pandas()  # tqdm과 pandas 연동
df[['lat', 'lon']] = df.progress_apply(convert_to_lat_lon, axis=1)

# 기존 x, y 열을 lat, lon으로 대체
df.drop(columns=['x', 'y'], inplace=True)

# 변환된 데이터 저장
output_file_path = "output_with_lat_lon.parquet"
df.to_parquet(output_file_path, engine='pyarrow')

print(f"변환된 데이터가 저장되었습니다: {output_file_path}")
