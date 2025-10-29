import sys
from cryptography.fernet import Fernet
from pyspark import SparkConf, SparkContext
import subprocess
import requests

conf = SparkConf().setAppName("WordCountApp").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

def load_key():
    """Load the Fernet encryption key from file."""
    with open("fernet.key", "rb") as f:
        return f.read()


def Extract(url, hdfs_path, filename):
    """Download file from URL directly into HDFS input directory."""
    hdfs_file_path = f"{hdfs_path}/{filename}"
    print(f"[Extract] Downloading {url} → {hdfs_file_path}")

    subprocess.run(["hadoop", "fs", "-mkdir", "-p", hdfs_path], check=False)
    subprocess.run(["hadoop", "fs", "-rm", "-f", hdfs_file_path], check=False)

    #cmd = f"curl -L -s {url} | hadoop fs -put - {hdfs_file_path}"
    #subprocess.run(cmd, shell=True, check=True)
    #Remove "shell=True" usage and use requests instead

    with requests.get(url, stream=True, verify=True, timeout=60) as r:
        r.raise_for_status()
        proc = subprocess.Popen(
            ["hadoop", "fs", "-put", "-", hdfs_file_path],
            stdin=subprocess.PIPE
        )
        for chunk in r.iter_content(chunk_size=8192):
            proc.stdin.write(chunk)
        proc.stdin.close()
        proc.wait()

    print("[Extract] Download complete:", hdfs_file_path)
    return hdfs_file_path



def Transform(hdfs_input_file, hdfs_output_dir):
    """Perform word count on HDFS file and write results to output_dir, including total count at the top."""
    print("[Transform] Starting word count...")

    subprocess.run(["hadoop", "fs", "-mkdir", "-p", hdfs_output_dir], check=False)

    text_file = sc.textFile(hdfs_input_file)

    counts = (
        text_file.flatMap(lambda line: line.lower().split())
        .map(lambda word: (word.strip('.,!?:;"()[]'), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: -x[1])
    )

    total_count = int(counts.map(lambda x: x[1]).sum())

    # Encrypt the total_count
    key = load_key()
    fernet = Fernet(key)
    encrypted_total = fernet.encrypt(str(total_count).encode()).decode()

    temp_output = f"{hdfs_output_dir}/temp_output"
    subprocess.run(["hadoop", "fs", "-rm", "-r", "-f", temp_output], check=False)
    subprocess.run(["hadoop", "fs", "-rm", "-f", f"{hdfs_output_dir}/output.txt"], check=False)

    counts.saveAsTextFile(temp_output)

    # Write encrypted total count first
    subprocess.run(
        ["bash", "-c", f"echo 'Encrypted total words: {encrypted_total}' | hadoop fs -put - {hdfs_output_dir}/output.txt"],
        check=True
    )

    # Append word counts
    subprocess.run(
        ["bash", "-c", f"hadoop fs -cat {temp_output}/* | hadoop fs -appendToFile - {hdfs_output_dir}/output.txt"],
        check=True
    )


def Load(hdfs_output_dir):
    """Display word count results."""
    print("\n[Load] Showing results:\n")
    #subprocess.run(f"hadoop fs -cat {hdfs_output_dir}/output.txt", shell=True, check=True)
    # Capture output instead of printing directly
    result = subprocess.run(
        ["hadoop", "fs", "-cat", f"{hdfs_output_dir}/output.txt"],
        check=True,
        capture_output=True,
        text=True
    )

    lines = result.stdout.strip().splitlines()

    # Decrypt the first line if it contains the encrypted total
    key = load_key()
    fernet = Fernet(key)

    if lines and lines[0].startswith("Encrypted total words:"):
        encrypted_value = lines[0].split(":", 1)[1].strip()
        try:
            decrypted_total = fernet.decrypt(encrypted_value.encode()).decode()
            print(f"Total words (decrypted): {decrypted_total}\n")
        except Exception as e:
            print(f"[Error decrypting total words]: {e}\n")

        # Print the rest of the file (word counts)
        print("\n".join(lines[1:]))
    else:
        print(result.stdout)


if __name__ == "__main__":
    url = "https://raw.githubusercontent.com/FilipePires98/LargeText-WordCount/main/datasets/AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt"

    hdfs_input_path = "hdfs://localhost:9000/user/loke/input_dir"
    hdfs_output_path = "hdfs://localhost:9000/user/loke/output_dir"
    input_filename = "AChristmasCarol_CharlesDickens_English.txt"

    hdfs_input_file = f"{hdfs_input_path}/{input_filename}"

    Extract(url, hdfs_input_path, input_filename)
    Transform(hdfs_input_file, hdfs_output_path)
    Load(hdfs_output_path)

    sc.stop()
