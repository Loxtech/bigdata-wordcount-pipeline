import requests
from hdfs import InsecureClient

def extract_to_hdfs(url, hdfs_path, filename):
    hdfs_file_path = f"{hdfs_path}/{filename}"

    # Remove existing file if exists
    if client.status(hdfs_file_path, strict=False):
        client.delete(hdfs_file_path)

    # Stream download and write to HDFS
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with client.write(hdfs_file_path, encoding='utf-8') as writer:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                writer.write(chunk.decode("utf-8"))

    print(f"Downloaded {filename} to HDFS at {hdfs_file_path}.")

    if __name__ == "__main__":
        url = "https://raw.githubusercontent.com/FilipePires98/LargeText-WordCount/main/datasets/AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt"

        hdfs_input_path = "/user/loke/input_dir"
        input_filename = "AChristmasCarol_CharlesDickens_English.txt"
        hdfs_output_path = "/user/loke/output_dir"

        extract_to_hdfs(url, hdfs_input_path, input_filename)
