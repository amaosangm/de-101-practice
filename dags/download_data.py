import requests
import zipfile
import os

data_lake_path = '/Users/linhao.jia/Documents/TWCode/DE/de-101-practice/data_lake'

# 定义下载和解压缩函数
def download_and_extract_zip(url, local_zip_path, extract_to):
    # 下载 ZIP 文件
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)

    # 解压缩 ZIP 文件
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    # 删除 ZIP 文件，节省空间
    os.remove(local_zip_path)


def download_backblaze_data():
    base_url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/"
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']

    # 遍历2019到2023年的每个季度
    for year in range(2019, 2024):
        for quarter in quarters:
            if year == 2023 and quarter == 'Q4':
                break  # 跳过2023 Q4
            zip_filename = f"data_{quarter}_{year}.zip"
            url = base_url + zip_filename
            local_zip_path = os.path.join(data_lake_path, zip_filename)

            # 下载并解压 ZIP 文件
            download_and_extract_zip(url, local_zip_path, data_lake_path)
