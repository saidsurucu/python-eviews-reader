import struct
import pandas as pd
import numpy as np

def read_eviews_header(filepath):
    with open(filepath, 'rb') as file:
        identifier = file.read(24).decode('utf-8', errors='replace')
        file.seek(80)
        header_size = struct.unpack('<Q', file.read(8))[0]
        file.seek(114)
        num_variables = struct.unpack('<I', file.read(4))[0] - 1
        file.seek(140)
        num_observations = struct.unpack('<I', file.read(4))[0]
        file.seek(124)
        frequency = struct.unpack('<H', file.read(2))[0]
        file.seek(126)
        start_subperiod = struct.unpack('<H', file.read(2))[0]
        file.seek(128)
        start_obs = struct.unpack('<I', file.read(4))[0]
        
        return {
            "identifier": identifier,
            "header_size": header_size,
            "num_variables": num_variables,
            "num_observations": num_observations,
            "frequency": frequency,
            "start_subperiod": start_subperiod,
            "start_obs": start_obs
        }

def read_variable_records(filepath, header_size, num_variables, offset_adjustment=0):
    with open(filepath, 'rb') as file:
        file.seek(header_size + 24 + offset_adjustment)
        variable_records = []
        for i in range(num_variables):
            record = file.read(70)
            code = struct.unpack('<H', record[62:64])[0]
            if code == 44:
                name_bytes = record[22:54].split(b'\x00')[0]
                name = name_bytes.decode('utf-8', errors='replace')
                data_size = struct.unpack('<I', record[10:14])[0]
                data_offset = struct.unpack('<Q', record[14:22])[0]
                
                variable_records.append({
                    "name": name,
                    "data_size": data_size,
                    "data_offset": data_offset
                })
        
        return variable_records

def extract_data_blocks(filepath, variable_records, num_observations):
    with open(filepath, 'rb') as file:
        data = {}
        for record in variable_records:
            name = record["name"]
            data_size = record["data_size"]
            data_offset = record["data_offset"]
            
            file.seek(data_offset)
            data_block = file.read(data_size)
            num_doubles = (data_size - 22) // 8
            if len(data_block) >= 22 + num_doubles * 8:
                try:
                    data_values = struct.unpack('<' + 'd' * num_doubles, data_block[22:22 + num_doubles * 8])
                    data[name] = [np.nan if val == 1e-37 else val for val in data_values]
                except struct.error as e:
                    print(f"  Error unpacking data block for {name}: {e}")
        
        return pd.DataFrame(data)
