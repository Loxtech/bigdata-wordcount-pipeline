import sys
from hdfs import InsecureClient

# Initialize HDFS client
client = InsecureClient("http://localhost:9870", user="loke")

def load_from_hdfs(hdfs_output_path):
    """
    Reads all part-files from the HDFS output directory and prints them to stdout.
    """
    try:
        # Check if the output directory exists
        if not client.status(hdfs_output_path, strict=False):
            print(f"Error: Output directory not found at {hdfs_output_path}", file=sys.stderr)
            return

        # Read and print each part-file
        for file_status in client.list(hdfs_output_path):
            if file_status[0].startswith('part-'):
                with client.read(f"{hdfs_output_path}/{file_status[0]}", encoding='utf-8') as reader:
                    print(reader.read(), end='')
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python Load.py <hdfs_output_path>", file=sys.stderr)
        sys.exit(1)
    
    load_from_hdfs(sys.argv[1])
      