import pandas as pd
import numpy as np
import re

class DataIngestor:
    def __init__(self, config):
        self.config = config
        self.error_log = []
        
    def _transform_logic(self, df, source_col, transforms):
        for action_entry in transforms:
            parts = action_entry.split(':')
            action = parts[0].upper()
            param = parts[1] if len(parts) > 1 else None

            # --- String Functions ---
            if action == "TRIM": df[source_col] = df[source_col].astype(str).str.strip()
            elif action == "UPPER": df[source_col] = df[source_col].astype(str).str.upper()
            elif action == "CAPITALIZE": df[source_col] = df[source_col].astype(str).str.capitalize()
            elif action == "LEFT" and param: df[source_col] = df[source_col].astype(str).str[:int(param)]
            elif action == "LPAD" and param:
                l_v, l_c = param.split('|')
                df[source_col] = df[source_col].astype(str).str.rjust(int(l_v), l_c)
            elif action == "REPLACE" and param:
                old_v, new_v = param.split('|')
                df[source_col] = df[source_col].astype(str).str.replace(old_v, new_v, regex=False)
            elif action == "CLEAN_PHONE":
                df[source_col] = df[source_col].astype(str).apply(lambda x: re.sub(r'\D', '', x))
            
            # --- SQL Logic ---
            elif action == "COALESCE":
                df[source_col] = df[source_col].replace(['nan', 'None', 'NaN', np.nan], param).fillna(param)
            elif action == "CAST" and param:
                if param == "INT": df[source_col] = pd.to_numeric(df[source_col], errors='coerce').fillna(0).astype(int)
                elif param == "FLOAT": df[source_col] = pd.to_numeric(df[source_col], errors='coerce').fillna(0.0)
            elif action == "DIVIDE" and param:
                df[source_col] = pd.to_numeric(df[source_col], errors='coerce') / float(param)

            # --- Date ---
            elif action == "DT_TO_YYMMDD":
                df[source_col] = pd.to_datetime(df[source_col], errors='coerce').dt.strftime('%y%m%d')

            # --- Validation ---
            elif action == "VALIDATE" and param:
                if param == "NUMERIC":
                    mask = pd.to_numeric(df[source_col], errors='coerce').isna()
                    if mask.any(): self.error_log.append(f"Field '{source_col}': Non-numeric data found.")
                elif param == "NOT_NULL":
                    if df[source_col].replace('', np.nan).isna().any():
                        self.error_log.append(f"Field '{source_col}': NULL/Blank values found.")
        return df

    def process(self, input_df):
        final_cols = {}
        working_df = input_df.copy()
        for item in self.config['mappings']:
            src, alias = item['source'], item['alias']
            if src in working_df.columns:
                working_df = self._transform_logic(working_df, src, item.get('transform', []))
                final_cols[src] = alias
        if self.error_log:
            print("\n--- DATA QUALITY REPORT ---")
            for msg in set(self.error_log): print(f"[!] {msg}")
        return working_df[list(final_cols.keys())].rename(columns=final_cols)