import PyPDF2
import json
from importlib import reload
from typing import Any, Dict, List
from urllib.parse import urlparse, unquote

import constants

reload(constants)
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
    TABLE_APP_TOKEN,
    TABLE_ID,
    SHEET_TOKEN,
    SHEET_ID,
    READ_RANGE,
    WRITE_RANGE,
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


def rate_papers(sop_content: str, tag_content: str, paper_links: List[str] = None, pdf_content: str = None) -> List[Dict[str, Any]]:
    """对论文列表进行评分

    Args:
        paper_links (List[str]): 论文链接列表
        sop_content (str): 评分标准内容链接
        tag_content (str): 岗位tag内容链接
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
            messages = get_rating_prompt(sop_content, tag_content, link, is_pdf = False)
            print(messages)
            lark.logger.info(f"messages: {messages}")
            # 调用 AI 进行评分
            completion = client.chat.completions.create(
                model=BOT_ID,
                messages=messages,
            )

            # 解析评分结果
            ai_ret = completion.choices[0].message.content.strip()
            lark.logger.info(f"ai_ret: {ai_ret}")
            #print("ai_ret内容:", ai_ret)
            ai_ret = re.sub(r'^<\|FunctionCallEnd\|>', '', ai_ret)
            
            #print("待解析的ai_ret内容：", ai_ret)
            try:
                result = json.loads(ai_ret)
            except json.JSONDecodeError as e:
                print(f"解析JSON出错：{e}")
                # 这里可以根据实际需求决定后续处理，比如返回默认值、记录日志等
                result = None
            
            result = json.loads(ai_ret)
            result["link"] = {"link": link, "text": link}
            results.append(result)

    #处理pdf文件上传（一般用于处理单个pdf上传的特例）
    if pdf_content:
        # 构造评分标准
        messages = get_rating_prompt(sop_content, tag_content, pdf_content, is_pdf = True)

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


def save_to_feishu_duowei(results: List[Dict[str, Any]]) -> None:
    """将评分结果保存到飞书多维表格

    Args:
        results (List[Dict[str, Any]]): 评分结果列表
    """
    # 获取访问令牌
    user_access_token = get_access_token(APP_ID, APP_SECRET)["access_token"]

    # 将结果保存到飞书多维表格
    add_records_to_dowei(TABLE_APP_TOKEN, TABLE_ID, user_access_token, results)

def save_to_feishu_sheet(spreadsheet_token, sheet_id, range, results: list[list[any]]) -> None:
    """将所需要的结果保存到飞书电子表格

    Args:
        spreadsheet_token: 表格token
        sheet_id: 工作表id
        range: 添加数据的范围，如A1:B2
        results (List[Dict[str, Any]]): 评分结果列表
    """
    # 获取访问令牌
    user_access_token = get_access_token(APP_ID,APP_SECRET)["access_token"]

    # 将结果保存到飞书电子表格
    add_records_to_feishu_sheet(spreadsheet_token, sheet_id, range, user_access_token, results)




def main():
    # 论文链接列表
    #paper_links = [
        #"https://arxiv.org/pdf/2506.09033"
        #"https://arxiv.org/pdf/2506.14245"
        #"https://arxiv.org/pdf/2409.09214"
        #"https://arxiv.org/pdf/2504.11346"
        #"https://arxiv.org/pdf/2502.14282",
        #"https://arxiv.org/pdf/2501.00663",
    #]


    #paper_links = [
    #"https://arxiv.org/pdf/2506.15461"
    #"https://arxiv.org/pdf/2506.14866"
    #"https://arxiv.org/pdf/2506.15455"
    #"https://arxiv.org/pdf/2506.09049"
    #"https://arxiv.org/pdf/2506.16406"
    #"https://arxiv.org/pdf/2506.15925"
    #"https://arxiv.org/pdf/2506.16054"
    #"https://arxiv.org/pdf/2506.17201"
    #"https://arxiv.org/pdf/2506.09033"
    #"https://arxiv.org/pdf/2506.14245"
    #"https://arxiv.org/pdf/2501.00663"
    #"https://arxiv.org/pdf/2502.14282"
    #"https://arxiv.org/pdf/2505.03335"
    #]

    #paper_links = get_huggingface_daily_papers_arxiv_links()

    #paper_content = extract_pdf_content("file:///C:/Users/Admin/Desktop/papers/nature14539.pdf")

    paper_links = get_arxiv_paper_links()[0:1]


    # 获取评分标准
    access_token = get_access_token(APP_ID, APP_SECRET)["access_token"]
    sop_content = get_feishu_doc_content(RATING_SOP_DOC_TOKEN, access_token)
    tag_content = get_feishu_doc_content(JOB_TAG_DOC_TOKEN, access_token)

    # 对论文进行评分
    results = rate_papers(sop_content, tag_content, paper_links = paper_links)
    with open("results.json", "w", encoding='utf-8', errors='ignore') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    # 保存结果到飞书
    save_to_feishu_duowei(results)


if __name__ == "__main__":
    main()
