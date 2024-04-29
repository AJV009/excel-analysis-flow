import os
from dotenv import load_dotenv
import pandas as pd
from difflib import SequenceMatcher
import requests
import json
import base64

load_dotenv()

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def usage_calculator_agent(run_name, usage, cuuid, total=False):
    input_cost_factor = float(os.environ.get("OPENAI_INPUT_COST", 0.01))
    output_cost_factor = float(os.environ.get("OPENAI_OUTPUT_COST", 0.03))

    input_tokens = usage["prompt_tokens"]
    input_cost = round(input_tokens / 1000 * input_cost_factor, 4)
    output_tokens = usage["completion_tokens"]
    output_cost = round(output_tokens / 1000 * output_cost_factor, 4)
    total_tokens = usage["total_tokens"]
    total_cost = round(total_tokens / 1000 * (input_cost_factor + output_cost_factor), 4)

    run_data = {
        "run_name": run_name,
        "usage": {
            "input_tokens": input_tokens,
            "input_cost": f"${input_cost:.4f}",
            "output_tokens": output_tokens,
            "output_cost": f"${output_cost:.4f}",
            "total_tokens": total_tokens,
            "total_cost": f"${total_cost:.4f}"
        }
    }

    file_path = f"temp_files/{cuuid}/llm_usage.json"

    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
    else:
        data = []

    data.append(run_data)

    if total:
        total_input_tokens = sum(run["usage"]["input_tokens"] for run in data)
        total_output_tokens = sum(run["usage"]["output_tokens"] for run in data)
        total_total_tokens = sum(run["usage"]["total_tokens"] for run in data)
        total_input_cost = round(total_input_tokens / 1000 * input_cost_factor, 4)
        total_output_cost = round(total_output_tokens / 1000 * output_cost_factor, 4)
        total_total_cost = round(total_total_tokens / 1000 * (input_cost_factor + output_cost_factor), 4)

        total_data = {
            "run_name": "total_use",
            "usage": {
                "input_tokens": total_input_tokens,
                "input_cost": f"${total_input_cost:.4f}",
                "output_tokens": total_output_tokens,
                "output_cost": f"${total_output_cost:.4f}",
                "total_tokens": total_total_tokens,
                "total_cost": f"${total_total_cost:.4f}"
            }
        }

        data.append(total_data)

    with open(file_path, "w") as file:
        json.dump(data, file, indent=2)

def reset_usage(cuuid):
    file_path = f"temp_files/{cuuid}/llm_usage.json"
    if os.path.exists(file_path):
        os.remove(file_path)

def return_total_usage_cost(cuuid):
    file_path = f"temp_files/{cuuid}/llm_usage.json"
    if os.path.exists(file_path):
        data = []
        with open(file_path, "r") as file:
            data = json.load(file)
        # iterate through the data and sum the total cost
        total_cost = 0
        for run in data:
            total_cost += float(run["usage"]["total_cost"].replace("$", ""))
        return total_cost
    else:
        return "No usage data found"

def small_sheet_query_agent(question, message_image_array, cuuid):
    print("Running small sheet query agent")
    system_prompt = """
You are an analytics bot that helps in analyzing documents.

Keep the following in mind:
- You will be given a set of documents and asked a question about them.
- Answer only if you are confident about the answer. 
- You will respond in a json format.
- If the document does not contain the answer, just add "no_answer_found" to the answer key.

The json format is as follows:
{"answer": "The answer to the question", "confidence": 0.9}

If you can't answer the question, just return:
{"answer": "no_answer_found", "confidence": 0}

Respond in pure directly parsaable json format.
"""

    question_frame = f"""
Carefully review the attached images, tables and data.
Based on the information provided, please answer the following question: " {question} "
"""

    headers = {
        "Authorization": "Bearer " + os.getenv('OPENAI_API_KEY'),
        "Content-Type": "application/json",
    }
    
    complete_payload = {
        "model": os.getenv('OPENAI_GEN_MODEL'),
        "temperature": 0.3,
        "response_format": { "type": "json_object" },
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": question_frame
                    }
                ] + message_image_array
            }
        ]
    }
    file_logger("small_sheet_query_agent - complete_payload", complete_payload, cuuid)
    response = requests.post(os.getenv('OPENAI_CHAT_URL'), headers=headers, json=complete_payload)
    file_logger("small_sheet_query_agent", response.json(), cuuid)
    usage_calculator_agent("small_sheet_agent", response.json()["usage"], cuuid)
    return json.loads(response.json()['choices'][0]['message']['content'])['answer']

def find_approx_text(metadata, search_text, threshold=0.8):
    csv_files = []
    for file_path in metadata['big_sheets']['csv_file_meta']:
        csv_files.append(file_path['csv_file_path'])
    df_dict = {f: pd.read_csv(f) for f in csv_files}
    
    results = {}
    
    for filename, df in df_dict.items():
        for col in df.columns:
            for idx, row in df.iterrows():
                cell_value = str(row[col])
                words = cell_value.split()

                for i in range(len(words)):
                    for j in range(i+1, len(words)+1):
                        substring = ' '.join(words[i:j])
                        similarity = SequenceMatcher(None, search_text, substring).ratio()

                        if similarity >= threshold:
                            key = (filename, idx, col)
                            if key not in results or similarity > results[key][1]:
                                results[key] = (substring, similarity)
    final_results = []
    for (filename, idx, col), (substring, similarity) in results.items():
        final_results.append({
            'filename': filename,
            'row': idx,
            'column': col,
            'substring': substring,
            'similarity': similarity
        })
    return final_results

def search_term_extraction_agent(query, cuuid):
    print("Running search term extraction agent")
    system_prompt = """
You are a search term extraction bot that helps in extracting search terms from queries to aid in better query processing and database search.

You have to keep the following in mind:
- You will be given a query that will contain some search word or string.
- You have extract the search term from the query.
- You will respond in a json format.
- If you can't find any valid search term in the the question, just return "no_search_term_found".

The json format is as follows:
{"search": "The answer to the question"}

If you can't find any valid search term in the the question, just return:
{"search": "no_search_term_found"}

Respond in pure directly parsaable json format.
"""

    question_frame = f"""
Following is a query to review and extract the search term:
" {{query}} "
"""

    fewshot_search_term_pairs = [
        {
            "query": question_frame.format(query="What was the issue date of tender bond for 'TENDER-Construction of Internal Roads at Airport Showroom Zone at Duqm (Phase 1)'?"),
            "search_term": "{\"search\": \"TENDER-Construction of Internal Roads at Airport Showroom Zone at Duqm (Phase 1)\"}"
        },
        {
            "query": question_frame.format(query="How many tenders were win in year 2016?"),
            "search_term": "{\"search\": \"no_search_term_found\"}"
        },
        {
            "query": question_frame.format(query="What was the submission date for “TENDER- Lulu at Palm Mall”?"),
            "search_term": "{\"search\": \"TENDER- Lulu at Palm Mall\"}"
        },
        {
            "query": question_frame.format(query="How many tenders were lost in year 2018?"),
            "search_term": "{\"search\": \"no_search_term_found\"}"
        }
    ]
    
    few_shot_messages = []
    for shot in fewshot_search_term_pairs:
        few_shot_messages.append({
            "role": "user",
            "content": shot['query']
        })
        few_shot_messages.append({
            "role": "assistant",
            "content": shot['search_term']
        })
        
    headers = {
        "Authorization": "Bearer " + os.getenv('OPENAI_API_KEY'),
        "Content-Type": "application/json",
    }
    
    complete_payload = {
        "model": os.getenv('OPENAI_GEN_MODEL'),
        "temperature": 0.3,
        "response_format": { "type": "json_object" },
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            *few_shot_messages,
            {
                "role": "user",
                "content": question_frame.format(query=query)
            }
        ]
    }
    file_logger("search_term_extraction_agent - complete_payload", complete_payload, cuuid)
    response = requests.post(os.getenv('OPENAI_CHAT_URL'), headers=headers, json=complete_payload)
    file_logger("search_term_extraction_agent", response.json(), cuuid)
    usage_calculator_agent("search_term_extraction_agent", response.json()["usage"], cuuid)
    return json.loads(response.json()['choices'][0]['message']['content'])['search']

def subterm_list(metadata, search_term):
    approx_terms = find_approx_text(metadata, search_term)
    substrings = set([term['substring'] for term in approx_terms])
    substrings_string = ""
    for substring in substrings:
        substrings_string += f'\n - "{substring}"'
    return substrings_string

def table_list(metadata, search_term):
    approx_terms = find_approx_text(metadata, search_term)
    tables = set([term['filename'] for term in approx_terms])
    tables = set([table.split('/')[-1].split('.')[0] for table in tables])
    return tables

def search_term_query_correction_agent(query, search_term, metadata, cuuid):
    print("Running search term query correction agent")
    system_prompt = """
You are a search query correction bot that helps in fixing query params to aid in better and accurate database search.

You have to keep the following in mind:
- You will be given a query that will contain some search word or string.
- You will also be given a list of possible highest matching substrings from the database.
- You have to replace the search term in the query with the correct term from the list of substrings.
- You will respond in a json format.
- Only create one response for each query. Despite the number of possible correct terms, you have to choose the best one that matches the query.

The json format is as follows:
{"query": "This is the 'corrected term' query"}

Respond in pure directly parsaable json format.
"""

    fewshot_search_term_pairs = [
        {
            "query": f"""Following is a query to review and correct the search term in it:
"What was the issue date of tender bond for 'TENDER-Construction of Internal Roads at Airport Showroom Zone at Duqm (Phase 1)'?"
            
Following are the possible correct search terms: 
{subterm_list(metadata,"TENDER-Construction of Internal Roads at Airport Showroom Zone at Duqm (Phase 1)")}
""",
            "search_term": "{\"query\": \"What was the issue date of tender bond for 'Construction of Internal Roads at Airport Showroom Zone at Duqm (Phase 1)'?\"}"
        },
        {
            "query": f"""Following is a query to review and correct the search term in it:
"What is the expiry date of tender bond for “TENDER BOND NAME - Fishery Harbour At Khasab-Onshore Facilities and Associated Infrastrucutre-Construction of Fish Auctation Hall, Guard House and Boundary Wall”?"

Following are the possible correct search terms:
{subterm_list(metadata,"TENDER BOND NAME - Fishery Harbour At Khasab-Onshore Facilities and Associated Infrastrucutre-Construction of Fish Auctation Hall, Guard House and Boundary Wall")}
""",
            "search_term": "{\"query\": \"What is the expiry date of tender bond for “Fishery Harbour At Khasab-Onshore Facilities and Associated Infrastrucutre-Construction of Fish Auctation Hall, Guard House and Boundary Wall”?\"}"
        },
        {
            "query": f"""Following is a query to review and correct the search term in it:
"What was the tender received date for “Civil Works for ESSAR at Duqm Refinery Phase 3 paCKAGE c (Tender)”?"

Following are the possible correct search terms:
{subterm_list(metadata, "Civil Works for ESSAR at Duqm Refinery Phase 3 paCKAGE c (Tender)")}
""",
            "search_term": "{\"query\": \"What was the tender received date for “Civil Works for ESSAR at Duqm Refinery Phase 3 paCKAGE c”?\"}"
        },
    ]

    few_shot_messages = []
    for shot in fewshot_search_term_pairs:
        few_shot_messages.append({
            "role": "user",
            "content": shot['query']
        })
        few_shot_messages.append({
            "role": "assistant",
            "content": shot['search_term']
        })
        
    headers = {
        "Authorization": "Bearer " + os.getenv('OPENAI_API_KEY'),
        "Content-Type": "application/json",
    }
    
    complete_payload = {
        "model": os.getenv('OPENAI_GEN_MODEL'),
        "temperature": 0.3,
        "response_format": { "type": "json_object" },
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            *few_shot_messages,
            {
                "role": "user",
                "content": f"""Following is a query to review and correct the search term in it:
"{query}"

Following are the possible correct search terms:
{subterm_list(metadata, search_term)}
"""
            }
        ]
    }
    file_logger("search_term_query_correction_agent - complete_payload", complete_payload, cuuid)
    response = requests.post(os.getenv('OPENAI_CHAT_URL'), headers=headers, json=complete_payload)
    file_logger("search_term_query_correction_agent", response.json(), cuuid)
    usage_calculator_agent("search_term_query_correction_agent", response.json()["usage"], cuuid)
    return json.loads(response.json()['choices'][0]['message']['content'])['query']

def query_writer_agent(query, table_mapping, cuuid):
    print("Running query writer agent")
    system_prompt = """
You are a SQL query bot that helps in querying the database.

Things to note:
- You will be given an image, table description from sqllite and a query to convert.
- The image is a sample of the table in the database. (head of the table)
- This sample will help you in understanding the table structure and column names.
- Write a SQL query to extract the required information from the table.
- You will respond in a json format.
- Encapculate all values in double quotes to garuntee the correct SQL query. (Table, Rows, Columns, Values, etc.)
- Anything other that SQL query has to be in double quotes to ensure correct parsing.

Final objective is to somhow convert the user query to an SQL query that will potensionally return the required information.

The json format is as follows:
{"query": "SELECT \"SOME COLUMN\" FROM \"table_name\" WHERE \"column_name\" = \"search_term\""}

Respond in pure directly parsaable json format.
"""

    multi_payload = []
    table_list = []
    for table_map in table_mapping:
        table_list.append(table_mapping[table_map]['table_name'])
        multi_payload.append({
            "model": os.getenv('OPENAI_GEN_MODEL'),
            "temperature": 0.3,
            "response_format": { "type": "json_object" },
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"""
Carefully review the attached image and table structure.
Based on the information provided, please formulate an SQL query for the following query: 
" {query} "

Table details:

Name: {table_mapping[table_map]['table_name']}

Columns:
```
{json.dumps(table_mapping[table_map]['column_details'], indent=4)}
```
"""
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{encode_image(table_mapping[table_map]['sample_image'])}"
                            }
                        }
                    ]    
                }
            ]
        })
    
    headers = {
        "Authorization": "Bearer " + os.getenv('OPENAI_API_KEY'),
        "Content-Type": "application/json",
    }
    
    multi_sql_queries = []
    i = 0
    for payload in multi_payload:
        file_logger(f"query_writer_agent_{i}_payload", payload, cuuid)
        response = requests.post(os.getenv('OPENAI_CHAT_URL'), headers=headers, json=payload)
        file_logger(f"query_writer_agent_{i}", response.json(), cuuid)
        usage_calculator_agent(f"query_writer_agent_{i}", response.json()["usage"], cuuid)
        multi_sql_queries.append({
            "table_name": table_list[i],
            "query": json.loads(response.json()['choices'][0]['message']['content'])['query']
        })
        i += 1
    return multi_sql_queries

def response_humanizer_agent(query, response, cuuid):
    print("Running response humanizer agent")

    file_logger("response_humanizer_agent - response param", response, cuuid)
    
    if not isinstance(response, str):
        response = json.dumps(response)

    system_prompt = """
You are a response humanizer bot that helps in converting raw answers and responses to a more human format.

Things to note:
- You will be given a query and a list of raw responses.
- The raw responses are generated by AI and may not be human readable.
- You have to convert the raw responses to a more human readable format.
- Do mention the running cost of the agents for each query in a separate line.
- You will respond in a json format.

The json format is as follows:
{"response": "The human readable response"}
"""

    fewshot_response_pairs = [
        {
            "payload": f"""Following is a query to review and humanize the response:
" What was the bid value of tender submitted for “TENDER-Civil Work for construction work of Central Stores and Work shop”? "

Following is the raw response:
```
[[152138.798]]
```
""",
            "response": """The bid value of tender submitted for “TENDER-Civil Work for construction work of Central Stores and Work shop” was $152,138.80"""
        },
        {
            "payload": f"""Following is a query to review and humanize the response:
" How many tenders have been submitted in year 2015? "

Following is the raw response:
```
31
```
""",
            "response": "31 tenders have been submitted in year 2015"
        }
        #TODO: add one more complex example here
    ]
    
    few_shot_messages = []
    for shot in fewshot_response_pairs:
        few_shot_messages.append({
            "role": "user",
            "content": shot['payload']
        })
        few_shot_messages.append({
            "role": "assistant",
            "content": shot['response']
        })
        
    headers = {
        "Authorization": "Bearer " + os.getenv('OPENAI_API_KEY'),
        "Content-Type": "application/json",
    }
    
    final_payload = {
        "model": os.getenv('OPENAI_GEN_MODEL'),
        "temperature": 0.3,
        "response_format": { "type": "json_object" },
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            *few_shot_messages,
            {
                "role": "user",
                "content": f"""Following is a query to review and humanize the response:
" {query} "

Following is the raw response:
```
{response}
```
"""
            }
        ]
    }
    file_logger("response_humanizer_agent - final_payload", final_payload, cuuid)
    response = requests.post(os.getenv('OPENAI_CHAT_URL'), headers=headers, json=final_payload)
    file_logger("response_humanizer_agent", response.json(), cuuid)
    usage_calculator_agent("response_humanizer_agent", response.json()["usage"], cuuid)
    return json.loads(response.json()['choices'][0]['message']['content'])['response']

def file_logger(process_name, data, cuuid):
    file_path = f"temp_files/{cuuid}/{cuuid}_logs.json"
    data = data.copy()

    if isinstance(data, str):
        data = {"timestamp": pd.Timestamp.now().isoformat(), "process": process_name, "data": data}
    elif isinstance(data, list):
        data.append({"timestamp": pd.Timestamp.now().isoformat(), "process": process_name})
    elif isinstance(data, dict):
        data["timestamp"] = pd.Timestamp.now().isoformat()
        data["process"] = process_name
    
    # open the file and append the data, then save it
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            logs = json.load(file)
    else:
        logs = []
    
    logs.append(data)
    
    with open(file_path, "w") as file:
        json.dump(logs, file, indent=2)

def reset_logs(cuuid):
    file_path = f"temp_files/{cuuid}/{cuuid}_logs.json"
    if os.path.exists(file_path):
        os.remove(file_path)
