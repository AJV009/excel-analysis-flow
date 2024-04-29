import base64
import os
import pandas as pd
import subprocess
import dataframe_image as dfi

def is_sheet_small(sheet, max_rows=40, max_cols=10, return_big_sheets=False):
    if sheet.max_row == 1 and sheet.max_column == 1 and sheet.cell(row=1, column=1).value is None:
        return False
    if return_big_sheets:
        return sheet.max_row > max_rows or sheet.max_column > max_cols
    return sheet.max_row <= max_rows and sheet.max_column <= max_cols
            
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def convert_to_pdf(input_file, current_uuid):
    import subprocess
    subprocess.run(['soffice',
                    '--headless',
                    '--convert-to', 'pdf:calc_pdf_Export:{"SinglePageSheets":{"type":"boolean","value":"true"}}',
                    input_file,
                    '--outdir', f'temp_files/{current_uuid}/'])
    
    output_file = input_file.replace('.xlsx', '.pdf')
    # check if output file exists
    if not os.path.exists(output_file):
        return None
    return output_file
                
def page_number_mapping(sheet_names):
    return {sheet_name: idx+1 for idx, sheet_name in enumerate(sheet_names)}

def get_img_from_pg_num(pdf_file, pg_num, page_mapping, current_uuid):
    # find sheet name from page number
    sheet_name = None
    for name, num in page_mapping.items():
        if num == pg_num:
            sheet_name = name
            break
    output_file = f'temp_files/{current_uuid}/sheet_{sheet_name}_{pg_num}.png'
    subprocess.run(['convert',
                    '-density', '300',
                    '-trim',
                    '-quality', '100',
                    f'{pdf_file}[{pg_num-1}]',
                    output_file])
    if not os.path.exists(output_file):
        print('Error: Image file not created')
        return
    return output_file

def convert_to_csv(sheet, file_path):
    # first row is the header
    df = pd.DataFrame(sheet.values)
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
    df.to_csv(file_path, index=False)
    if not os.path.exists(file_path):
        print('Error: CSV file not created')
        return
    return file_path

def get_img_from_csv(csv_file, pg_num, page_mapping, current_uuid):
    sheet_name = None
    for name, num in page_mapping.items():
        if num == pg_num:
            sheet_name = name
            break
    df = pd.read_csv(csv_file)
    sample_df = df.sample(n=5)
    output_file = f'temp_files/{current_uuid}/sample_sheet_{sheet_name}_{pg_num}.png'
    dfi.export(sample_df, output_file, max_cols=-1, table_conversion="selenium")
    if not os.path.exists(output_file):
        print('Error: Image file not created')
        return
    return output_file
