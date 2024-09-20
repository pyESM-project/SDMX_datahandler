from pathlib import Path
from typing import Optional
from pyjstat import pyjstat

import requests
import pandas as pd
import os


class EurostatAPI:

    def __init__(
            self, 
            base_url: str, 
            output_dir: Optional[str] = None,
        ) -> None:

        self.base_url = base_url
        self.output_dir = Path(output_dir)

        if not self.output_dir.exists():
            os.makedirs(self.output_dir)
    
    def build_query_url(
            self,
            query_params: dict[str, str],
    ) -> str:
        
        final_url = requests.Request(
            'GET', self.base_url, params=query_params
        ).prepare().url
        
        print(f"Final url: {final_url}")
        return final_url

    def fetch_data_to_dataframe(
            self, 
            query_params: Optional[dict[str, str]] = None,
    ) -> pd.DataFrame:

        try:
            response = requests.get(self.base_url, params=query_params)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')

            if 'application/json' in content_type:
                raw_data = response.json()
                dataframe: pd.DataFrame = pyjstat.from_json_stat(raw_data)[0]
            
            if not dataframe.empty:
                return dataframe

            print("Unsupported response format.")
            return None
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        
        except Exception as e:
            print(f"Error: {e}")
            return None

    def dataframe_to_file(
            self,
            data: pd.DataFrame,
            filename: str,
            file_format: Optional[str] = 'csv',
    ) -> None:
        
        allowed_formats = ['csv', 'txt']
        
        # case filename includes file extension
        if '.' in filename:
            filename, ext = filename.rsplit('.', 1)
            if ext not in allowed_formats:
                raise ValueError(
                    f"Unsupported file extension: {ext}. Allowed formats: " \
                    f"{allowed_formats}")
            if file_format and ext != file_format:
                raise ValueError(
                    f"File extension in filename {ext} differs from " \
                    f"the passed file_format {file_format}.")
            file_format = ext
        
        # case filename does not include file extension
        else:
            if file_format not in allowed_formats:
                raise ValueError(
                    f"Unsupported file format: {file_format}. Allowed " \
                    f"formats: {allowed_formats}")
            filename = f"{filename}.{file_format}"

        file_path = self.output_dir / filename

        if file_path.exists():
            overwrite = input(
                f"File {file_path} already exists. Overwriting file? y/[n]: ")
            if overwrite.lower() != 'y':
                print("Data not exported to file.")
                return

        try:
            if file_format == 'csv':
                data.to_csv(file_path, index=False)
            elif file_format == 'txt':
                data.to_csv(file_path, sep='\t', index=False)
            print(f"Data successfully saved to {file_path}")
        except IOError as e:
            print(f"Error saving data: {e}")

    def scrape_data(
            self, 
            query_params: dict[str, str],
            chunk_query_by: Optional[str] = None,
            filename: Optional[str] = None,
            file_format: Optional[str] = 'csv',
    ) -> None:
        
        print("Fetching data...")

        if chunk_query_by:
            if not chunk_query_by in query_params:
                print(
                    f"Query parameter '{chunk_query_by}' not found in " \
                    "query_params. Available parameters: " \
                    f"{list(query_params.keys())}")
                return
            
            dataframe = pd.DataFrame()
            for chunk in query_params[chunk_query_by]:
                query_params[chunk_query_by] = chunk              
                print(f"Fetching data for {chunk}...")
                chunk_dataframe = self.fetch_data_to_dataframe(query_params)
                
                if chunk_dataframe.empty:
                    print(f"Data not found for {chunk}. Skipping...")
                    continue
                dataframe = pd.concat([dataframe, chunk_dataframe])

        else:
            dataframe = self.fetch_data_to_dataframe(query_params)

        if dataframe.empty:
            print("No data found.")
            return
        else:
            print("Data fetched successfully.")

            if filename:
                self.dataframe_to_file(dataframe, filename, file_format)
                
            return dataframe