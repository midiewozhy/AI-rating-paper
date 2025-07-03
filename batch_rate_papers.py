import concurrent.futures
import PyPDF2
import json
from importlib import reload
from typing import Any, Dict, List
from urllib.parse import urlparse, unquote

import constants
import utils

reload(constants)
reload(utils)
import json
import os
import re

import lark_oapi as lark
from lark_oapi.api.docs.v1 import *
from openai import OpenAI


from constants import (
    APP_ID,
    APP_SECRET,
    ARK_API_KEY,
    BOT_ID,
    RATING_SOP_DOC_TOKEN,
    JOB_TAG_DOC_TOKEN,
    RELEVANCE_DOC_TOKEN,
    TABLE_APP_TOKEN,
    HUGGING_FACE_TABLE_ID,
    ARXIV_TABLE_ID,
    SHEET_TOKEN,
    SHEET_ID,
)
from utils import (
    add_records_to_dowei,
    add_records_to_feishu_sheet,
    get_access_token,
    get_feishu_doc_content,
    get_feishu_sheet_content,
    get_rating_prompt,
    get_huggingface_daily_papers_arxiv_links,
    get_arxiv_paper_links,
    get_feishu_sheet_content,
)

def extract_pdf_content(pdf_url: str) -> str:
    """
    从PDF文件中提取文本内容
    
    Args:
        pdf_path (str): PDF文件的路径
    
    Returns:
        str: 提取的文本内容，如果文件不存在或发生错误则返回空字符串
    """
    
    parsed = urlparse(pdf_url)
    path = unquote(parsed.path)
    
    # 处理 Windows 路径（去除开头的斜杠）
    if path.startswith("/") and len(path) > 3 and path[2] == ":":
        file_path = path[1:]  # 例如："/C:/path" → "C:/path"
    
    
    try:
        # 打开PDF文件
        with open(file_path, 'rb') as file:
            # 创建PDF阅读器对象
            pdf_reader = PyPDF2.PdfReader(file)
            
            # 初始化文本变量
            text = ""
            
            # 遍历所有页面
            for page_num in range(len(pdf_reader.pages)):
                # 获取当前页面
                page = pdf_reader.pages[page_num]
                
                # 提取当前页面的文本
                page_text = page.extract_text()
                
                # 添加到整体文本中
                text += page_text + "\n\n"  # 每页之间添加空行分隔
            
            return text.strip()  # 返回去除首尾空白的文本
            
    except FileNotFoundError:
        print(f"错误: 文件 '{file_path}' 不存在")
        return ""
    except Exception as e:
        print(f"错误: 读取PDF文件时发生异常 - {str(e)}")
        return ""


def rate_papers(sop_content: str, tag_content: str, date_str: str, relevance_content: str = None, paper_links: List[str] = None, pdf_content: str = None) -> List[Dict[str, Any]]:
    """对论文列表进行评分

    Args:
        paper_links (List[str]): 论文链接列表
        sop_content (str): 评分标准内容
        tag_content (str): 岗位tag内容
        relevance_content (str): 研究相关性内容
        pdf_content (str): HR单独投入的文章内容

    Returns:
        List[Dict[str, Any]]: 评分结果列表
    """
    # 初始化 OpenAI 客户端
    client = OpenAI(
        base_url="https://ark.cn-beijing.volces.com/api/v3/bots",
        api_key=ARK_API_KEY,
    )

    results = []
    
    #处理pdf url链接输入（一般用于批量化爬取的信息）
    if paper_links:
        for link in paper_links:
            # 构造评分提示
            messages = get_rating_prompt(sop_content, tag_content, link, False)
            lark.logger.info(f"messages: {messages}")
            # 调用 AI 进行评分
            try: 
                completion = client.chat.completions.create(
                    model=BOT_ID,
                    messages=messages,
                    # 以下参数调整的目的是使得打分的波动性低一点
                    temperature=0, 
                    top_p=0.9, # 在temperature = 0的情况下该参数无效
                    seed=42, # 固定随机种子
                    #max_tokens=150,
                )

                #检查api响应是否为空
                if not completion.choices or not completion.choices[0].message.content:
                    lark.logger.error(f"API响应内容为空，跳过论文: {link}")
                    continue

                # 解析评分结果
                ai_ret = completion.choices[0].message.content.strip()
                lark.logger.info(f"ai_ret: {ai_ret}")
                ai_ret = re.sub(r'^(<\|FunctionCallEnd\|>|```json\n?|```\n?)', '', ai_ret, flags=re.IGNORECASE)
                ai_ret = re.sub(r'```\s*$', '', ai_ret)  # 移除结尾的代码块标记

                #检查是否为空内容
                if not ai_ret:
                    lark.logger.error(f"清理后内容为空，跳过论文: {link}")
                    continue
                
                #添加所需字段
                result = json.loads(ai_ret)
                result["link"] = {"link": link, "text": link}
                result["date"] = date_str
                results.append(result)

            except json.JSONDecodeError as e:
                lark.logger.error(f"解析JSON出错：{e}，内容：{ai_ret}，跳过论文: {link}")
            except Exception as e:
                lark.logger.error(f"处理论文时发生意外错误：{type(e).__name__} - {str(e)}，跳过论文: {link}")
            

    #处理pdf文件上传（一般用于处理单个pdf上传的特例）
    if pdf_content:
        # 构造评分标准
        messages = get_rating_prompt(sop_content, tag_content, relevance_content, pdf_content, True)

        lark.logger.info(f"messages:{messages}")
        # 调用AI评分
        completion = client.chat.completions.create(
                model=BOT_ID,
                messages=messages,
            )
        # 解析评分结果
        ai_ret = completion.choices[0].message.content.strip()
        lark.logger.info(f"ai_ret: {ai_ret}")
        ai_ret = re.sub(r'^<\|FunctionCallEnd\|>', '', ai_ret)
            
        try:
            result = json.loads(ai_ret)
        except json.JSONDecodeError as e:
            print(f"解析JSON出错：{e}")
            # 这里可以根据实际需求决定后续处理，比如返回默认值、记录日志等
            result = None
            
        result = json.loads(ai_ret)
        #result["link"] = {"link": link, "text": link}
        results.append(result)

    return results

#通用的处理数据并让大模型生成分析的函数
def process_source(results, date, sop_content, tag_content, table_id, source_name, relevance_content = None):
    """处理单个数据源的通用函数"""
    if results:
        date = date
        #results = results[0:3]  # 切片的目的只是用来测试，后期需要删除再投入实际使用
        analysis = rate_papers(sop_content, tag_content, date, paper_links=results)
        save_to_feishu_duowei(analysis, table_id)
        # 保存到本地文件
        with open(f"{source_name}_results.json", "w", encoding='utf-8', errors='ignore') as f:
            json.dump(analysis, f, indent=4, ensure_ascii=False)
        return analysis
    return None


def save_to_feishu_duowei(results: List[Dict[str, Any]], table_id: str) -> None:
    """将评分结果保存到飞书多维表格

    Args:
        results (List[Dict[str, Any]]): 评分结果列表
        table_id (str): 用来存储对应数据的table_id，不同数据存储地址不一样
    """
    # 获取访问令牌
    user_access_token = get_access_token(APP_ID, APP_SECRET)["access_token"]

    # 将结果保存到飞书多维表格
    add_records_to_dowei(TABLE_APP_TOKEN, table_id, user_access_token, results)

def save_to_feishu_sheet(spreadsheet_token, sheet_id, range, results: list[list[any]]) -> None:
    """将所需要的结果保存到飞书电子表格

    Args:
        spreadsheet_token: 表格token
        sheet_id: 工作表id
        range: 添加数据的范围，如A1:B2
        results (List[List[Any]]): 评分结果列表
    """
    # 获取访问令牌
    user_access_token = get_access_token(APP_ID,APP_SECRET)["access_token"]

    # 处理results格式问题
    cleaned_results = [
        [
            result['score'],
            result['summary'],
            result['tag_primary'],
            result['contact_tag_primary'],
            result['tag_secondary'],
            result['contact_tag_secondary'],
            result['是否有华人']
        ]
        for result in results
    ]


    # 将结果保存到飞书电子表格
    add_records_to_feishu_sheet(spreadsheet_token, sheet_id, range, user_access_token, cleaned_results)




def main():

    # 获取评分标准
    access_token = get_access_token(APP_ID, APP_SECRET)["access_token"]
    sop_content = get_feishu_doc_content(RATING_SOP_DOC_TOKEN, access_token)
    tag_content = get_feishu_doc_content(JOB_TAG_DOC_TOKEN, access_token)
    relevance_content = get_feishu_doc_content(RELEVANCE_DOC_TOKEN, access_token)

    # 并行爬取论文链接
    arxiv_results = None
    hf_results = None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        arxiv_future = executor.submit(get_arxiv_paper_links)
        hf_future = executor.submit(get_huggingface_daily_papers_arxiv_links)
        
        try:
            arxiv_results = arxiv_future.result(timeout=300)  # 设置超时时间为5分钟
        except Exception as e:
            print(f"爬取Arxiv论文链接时出错: {e}")
        
        try:
            hf_results = hf_future.result(timeout=300)  # 设置超时时间为5分钟
        except Exception as e:
            print(f"爬取Hugging Face论文链接时出错: {e}")

    # 获取论文数量
    arxiv_links = arxiv_results[0]
    hf_links = hf_results[0]
    arxiv_len = len(arxiv_links)
    hf_len = len(hf_links)
    total_papers = arxiv_len + hf_len
    arxiv_date = arxiv_results[1]
    hf_date = hf_results[1]
    
    # 如果没有论文则直接返回
    if total_papers == 0:
        print("没有获取到任何论文数据，跳过处理")
        return
    
    # 计算线程比例（基于论文数量）
    arxiv_ratio = arxiv_len / total_papers
    #hf_ratio = 1 - arxiv_ratio
    
    # 设置最大线程数（≤10）
    MAX_WORKERS = 10
    total_workers = min(MAX_WORKERS, max(3, total_papers // 10))  # 3-10个线程
    
    # 按比例分配线程（确保每类至少1线程）
    arxiv_workers = max(1, min(MAX_WORKERS-1, round(total_workers * arxiv_ratio))) if arxiv_len > 0 else 0
    hf_workers = max(1, total_workers - arxiv_workers) if hf_len > 0 else 0
    
    print(f"线程分配: arXiv={arxiv_workers}线程, HF={hf_workers}线程 (共{total_workers}线程)")

    # 使用统一线程池处理所有任务（更高效）
    with concurrent.futures.ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        
        # 处理arXiv论文（如果有）
        if arxiv_len > 0:
            arxiv_chunk_size = max(10, min(30, arxiv_len // max(1, arxiv_workers * 2) or 10))
            arxiv_chunks = [arxiv_links[i:i+arxiv_chunk_size] 
                          for i in range(0, arxiv_len, arxiv_chunk_size)]
            
            print(f"arXiv分块: {len(arxiv_chunks)}个任务块, 每块约{arxiv_chunk_size}篇论文")
            
            for chunk in arxiv_chunks:
                futures.append(executor.submit(
                    process_source, 
                    chunk, 
                    arxiv_date,
                    sop_content, 
                    tag_content,
                    ARXIV_TABLE_ID,
                    "arxiv"
                ))
        
        # 处理HF论文（如果有）
        if hf_len > 0:
            hf_chunk_size = max(5, min(20, hf_len // max(1, hf_workers * 2) or 5))
            hf_chunks = [hf_links[i:i+hf_chunk_size] 
                       for i in range(0, hf_len, hf_chunk_size)]
            
            print(f"HF分块: {len(hf_chunks)}个任务块, 每块约{hf_chunk_size}篇论文")
            
            for chunk in hf_chunks:
                futures.append(executor.submit(
                    process_source,
                    chunk,
                    hf_date,
                    sop_content,
                    tag_content,
                    HUGGING_FACE_TABLE_ID,
                    "hf"
                ))
        
        # 等待所有任务完成
        print(f"等待{len(futures)}个任务块处理完成...")
        concurrent.futures.wait(futures)
        save_to_feishu_duowei(dict(),ARXIV_TABLE_ID)
        save_to_feishu_duowei(dict(),HUGGING_FACE_TABLE_ID)
        print("所有论文处理任务完成")
    
    print("论文处理流程完成")    



if __name__ == "__main__":
    main()

