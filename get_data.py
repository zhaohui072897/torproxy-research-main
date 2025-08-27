import os
import zlib
import json
import base64

def read_deflate_file_line_by_line(file_path):
    # Read the compressed data from the .deflate file
    with open(file_path, 'rb') as file:
        compressed_data = file.read()

    # Decompress the data
    decompressed_data = zlib.decompress(compressed_data)

    # Convert the decompressed bytes to a string
    decompressed_text = decompressed_data.decode('utf-8')

    # Split the text into lines
    lines = decompressed_text.splitlines()

    # Convert each line to a JSON object and decode the 'content' field
    for idx, line in enumerate(lines, start=1):
        try:
            json_object = json.loads(line)
            
            if 'content' in json_object:
                # Decode the Base64 encoded 'content'
                decoded_content = base64.b64decode(json_object['content']).decode('utf-8')
                json_object['content'] = decoded_content
            
            yield json_object, idx
        except (json.JSONDecodeError, base64.binascii.Error, UnicodeDecodeError) as e:
            print(f"Error decoding JSON or Base64 or Unicode in line number {idx} : {e}")
            yield None, idx

def save_json_to_file(json_object, index, output_folder):
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Define the output file path
    output_file_path = os.path.join(output_folder, f"{index}.json")

    # Write the JSON object to the file
    with open(output_file_path, 'w') as file:
        json.dump(json_object, file, indent=4)

def pick_latest_deflate(data_pages_dir):
    """在 data_pages 目录下自动挑选“最新修改时间”的 .deflate 文件"""
    if not os.path.isdir(data_pages_dir):
        raise FileNotFoundError(f"目录不存在: {data_pages_dir}")
    candidates = [
        os.path.join(data_pages_dir, f)
        for f in os.listdir(data_pages_dir)
        if f.lower().endswith(".deflate")
    ]
    if not candidates:
        raise FileNotFoundError(f"未在 {data_pages_dir} 找到 .deflate 文件")
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


# Path to your .deflate file
# file_path = './server-test/king_digital_urls_6/data_pages/crawl_data-1755914686884-0.deflate'
# output_folder = 'cracking-to-json/premium-accounts'

# 自动定位 data_pages 目录下“最新”的 .deflate，并用其上一级目录名作为输出子目录名
data_pages_dir = './server-cracking/cracking_forum_urls_level_1_batch_13_1756283691/data_pages'  # ← 只改这一行到你的 data_pages 目录
file_path = pick_latest_deflate(data_pages_dir)

# 例如 .../server-test/king_digital_urls_6/data_pages → crawler_name = king_digital_urls_6
crawler_name = os.path.basename(os.path.dirname(data_pages_dir))
output_folder = os.path.join('cracking-to-json', crawler_name)
print(f"[INFO] 使用的 .deflate: {file_path}")
print(f"[INFO] 输出目录: {output_folder}")



# Read and process the .deflate file line by line
for json_object, index in read_deflate_file_line_by_line(file_path):
    if json_object is not None:
        save_json_to_file(json_object, index, output_folder)

