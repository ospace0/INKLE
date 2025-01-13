import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Parquet 파일 읽기
parquet_file = r"F:\INKLE\2024_01_10\daily_parquets\temp_parquet_hourly\satellite_data_1havg_20250108_00_277x306.parquet"
df = pd.read_parquet(parquet_file, engine = 'fastparquet')

# 테이블 형식으로 저장
def save_table_as_image(dataframe, image_path):
    fig, ax = plt.subplots(figsize=(12, len(dataframe) * 0.4))  # 데이터 크기에 따라 동적 크기 설정
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=dataframe.values, colLabels=dataframe.columns, loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.auto_set_column_width(col=list(range(len(dataframe.columns))))
    plt.savefig(image_path, bbox_inches='tight', dpi=300)
    plt.close()

save_table_as_image(df.head(20), "table_image.png")  # 상위 20개 행만 저장

# 그래프로 저장
def save_graph_as_image(dataframe, image_path):
    sns.set(style="whitegrid")
    plt.figure(figsize=(10, 6))
    sns.barplot(data=dataframe.head(10), x=dataframe.columns[0], y=dataframe.columns[1])  # 예시로 첫 두 열 사용
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(image_path, dpi=300)
    plt.close()

save_graph_as_image(df, "graph_image.png")  # 첫 두 열의 데이터로 예제 그래프 생성
