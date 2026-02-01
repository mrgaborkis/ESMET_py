import pandas as pd
import json
import os
from ingestor_engine import DataIngestor

def run_pipeline():
    base_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base_path, 'config.json'), 'r') as f:
        config = json.load(f)
    
    settings = config.get('settings', {})
    in_sep = settings.get('input_delimiter', '\t')
    out_sep = settings.get('output_delimiter', '|')

    # Load and Process
    input_file = os.path.join(base_path, 'test_input.csv')
    if not os.path.exists(input_file):
        print("Input file missing.")
        return

    df_raw = pd.read_csv(input_file, sep=in_sep)
    df_final = DataIngestor(config).process(df_raw)

    # Save
    output_file = os.path.join(base_path, 'final_output.txt')
    df_final.to_csv(output_file, sep=out_sep, index=False)
    print(f"\nETL Complete. Processed {len(df_final)} rows.")

if __name__ == "__main__":
    run_pipeline()