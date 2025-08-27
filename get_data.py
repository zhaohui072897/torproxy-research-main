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

# Path to your .deflate file
file_path = './server2/cracking_premium-accounts_list/data_pages/crawl_data-1721654792089-0.deflate'
output_folder = 'cracking-to-json/premium-accounts'

# Read and process the .deflate file line by line
for json_object, index in read_deflate_file_line_by_line(file_path):
    if json_object is not None:
        save_json_to_file(json_object, index, output_folder)

