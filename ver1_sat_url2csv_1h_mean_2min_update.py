import requests
import h5py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os

# 파일 다운로드 함수                                 
def download_file(file_url, save_path):
    with open(save_path, 'wb') as f:
        response = requests.get(file_url)
        f.write(response.content)

# 이미지 데이터를 CSV로 저장하는 함수
def process_image_to_csv(data_list, data_types, csv_save_path):
    if not data_list:
        print("수집된 데이터가 없습니다.")
        return

    size_grouped_data = {}
    for data_type_index, data_type in enumerate(data_types):
        all_data_for_type = []
        for file_data in data_list:
            if file_data is not None:
                image_data = file_data[data_type_index]
                if image_data is not None:
                    all_data_for_type.append(image_data)

        if not all_data_for_type:
            print(f"{data_type}에 대한 유효한 데이터가 없습니다.")
            continue

        rows, cols = all_data_for_type[0].shape #행,열 개수 세기
        x, y = np.meshgrid(range(0, cols), range(0, rows)) #좌표생성
        averaged_data = np.nanmean(np.stack(all_data_for_type), axis=0) #평균계산

        flattened_data = pd.DataFrame({
            'x': x.ravel(),
            'y': y.ravel(),
            data_type: averaged_data.ravel()
        })

        size_key = f"{rows}x{cols}"
        if size_key not in size_grouped_data:
            size_grouped_data[size_key] = flattened_data #같은 크기 없으면 새로운 파일
        else:
            size_grouped_data[size_key] = pd.merge(size_grouped_data[size_key], flattened_data, on=['x', 'y'], how='outer') #같은 크기 있으면 기존 파일에 병합합(기존 열 바로 옆에)

    for size_key, combined_data in size_grouped_data.items():
        csv_save_path_with_size = f"{csv_save_path}_{size_key}.csv"
        combined_data.to_csv(csv_save_path_with_size, index=False)
        print(f"CSV 파일이 {csv_save_path_with_size}에 저장되었습니다.")


def main():
    auth_key = "c4GK7IkiRoWBiuyJIhaFgQ"
    data_types = ["VI004", "VI005", "VI006", "VI008", "NR013", "NR016", "SW038", "WV063", "WV069", "WV073", "IR087", "IR096", "IR105", "IR112", "IR123", "IR133"]
    region = "LA" #지역
    start_date = datetime(2024, 7, 12)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    os.makedirs("D:/sat_file/temp", exist_ok=True)

    searching_date = start_date
    while searching_date <= end_date:
        hourly_data_list = []
        hourly_start_time = searching_date
        hourly_end_time = searching_date + timedelta(hours=1)
        data_collected_count = 0

        while searching_date < hourly_end_time:
            file_data_per_type = []
            searching_time = searching_date.replace(second=0, microsecond=0) #url은 초단위 인정 안함
            for data_type in data_types:
                url = f"https://apihub.kma.go.kr/api/typ05/api/GK2A/LE1B/{data_type}/{region}/data?date={searching_time.strftime('%Y%m%d%H%M')}&authKey={auth_key}"
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    ncfile_path = f"D:/sat_file/temp/satellite_data_{searching_time.strftime('%Y%m%d%H%M')}_{data_type}.nc"
                    with open(ncfile_path, 'wb') as f:
                        f.write(response.content)
                    with h5py.File(ncfile_path, 'r') as file:
                        image_data = file['image_pixel_values'][:]
                        file_data_per_type.append(image_data)
                    #os.remove(ncfile_path)
                except requests.exceptions.RequestException as e:
                    print(f"{data_type} 다운로드 오류: {e}")
                    file_data_per_type.append(None)
                except OSError as e:
                    print(f"파일 처리 오류 {e}")
                    file_data_per_type.append(None)
                except KeyError as e:
                    print(f"hdf5 파일 데이터 오류 {e}")
                    file_data_per_type.append(None)
            

            hourly_data_list.append(file_data_per_type)
            data_collected_count += 1
            print(f"{searching_date.strftime('%Y-%m-%d %H:%M:%S')} 데이터 수집 완료 ({data_collected_count}회)")
            searching_date += timedelta(minutes=30)
            time.sleep(1)
        timestamp = hourly_start_time.strftime('%Y%m%d_%H')
        csv_base_path = f"D:/sat_file/satellite_data_combined_{timestamp}"
        process_image_to_csv(hourly_data_list, data_types, csv_base_path)
        print(f"{hourly_start_time.strftime('%Y-%m-%d %H시')} 데이터 평균 계산 및 CSV 저장 완료")

if __name__ == '__main__':
    main()