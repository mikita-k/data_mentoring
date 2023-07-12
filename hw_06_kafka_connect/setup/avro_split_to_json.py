import json
import os

import fastavro


def convert_x_records_to_json_files(input_file, file_cnt):
    with open(input_file, 'rb') as f:
        reader = fastavro.reader(f)

        cnt = 0
        for i, record in enumerate(reader):
            cnt += 1
            if cnt > file_cnt:
                break

            file_path = f'data/json/record_{i}.json'
            if os.path.exists(file_path):
                continue

            json_data = json.dumps(record)
            with open(file_path, 'w') as json_file:
                json_file.write(json_data)

        print("Splitting completed.")


# Usage example
input_file = 'data/avro/expedia+0+0000000000.avro'  # Path to the input Avro file

convert_x_records_to_json_files(input_file, 20)
