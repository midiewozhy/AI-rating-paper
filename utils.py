import json
import time
from datetime import datetime, timedelta, timezone

import lark_oapi as lark
import requests
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.docs.v1 import *

from bs4 import BeautifulSoup
import re

import arxiv
from datetime import datetime, timedelta


def get_access_token(app_id, app_secret):
    """
    获取自定义应用的app_access_token
    :param app_id: 应用的唯一标识符
    :param app_secret: 应用的密钥
    :return: 包含app_access_token和过期时间的字典，失败时返回None
    """
    # 定义API请求的URL
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"

    # 设置请求头
    headers = {"Content-Type": "application/json; charset=utf-8"}

    # 构建请求体
    payload = {"app_id": app_id, "app_secret": app_secret}

    try:
        # 发送POST请求
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # 解析响应内容
        result = response.json()

        # 检查请求是否成功（code为0表示成功）
        if result.get("code") == 0:
            # 提取app_access_token和过期时间
            access_token = result.get("app_access_token")
            expire = result.get("expire")
            print(result)

            # 返回包含访问令牌和过期时间的字典
            return {
                "access_token": access_token,
                "expire": expire,
                "timestamp": int(time.time()),  # 添加获取时间戳
            }
        else:
            # 打印错误信息
            print(
                f"获取access_token失败，错误码：{result.get('code')}，错误信息：{result.get('msg')}"
            )
            return None
    except requests.exceptions.RequestException as e:
        # 处理请求异常
        print(f"请求异常：{e}")
        return None
    except json.JSONDecodeError as e:
        # 处理响应解析异常
        print(f"响应解析异常：{e}")
        return None


def get_feishu_doc_content(doc_token: str, access_token: str) -> str:
    """获取飞书文档内容

    Args:
        doc_token (str): 文档的 token
        access_token (str): 访问令牌

    Returns:
        str: 文档内容
    """
    # 创建client
    client = (
        lark.Client.builder()
        .enable_set_token(True)
        .log_level(lark.LogLevel.DEBUG)
        .build()
    )

    # 构造请求对象
    request: GetContentRequest = (
        GetContentRequest.builder()
        .doc_token(doc_token)
        .doc_type("docx")
        .content_type("markdown")
        .build()
    )

    # 发起请求
    option = lark.RequestOption.builder().user_access_token(access_token).build()
    response: GetContentResponse = client.docs.v1.content.get(request, option)

    # 处理失败返回
    if not response.success():
        error_msg = f"client.docs.v1.content.get failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
        raise Exception(error_msg)

    # 返回文档内容
    return response.data.content


def get_rating_prompt(sop_content: str, tag_content: str, paper_content: str,  is_pdf: bool) -> list:
    """获取评论文本

    Args:
        sop_content (str): SOP 内容
        paper_content (str): 论文链接或读取到的pdf内容

    Returns:
        str: 评论文本
    """

    system_prompt = f"""
    你是一个专业的评阅人，根据用户给定的论文链接，以 json 格式返回你的评分和总结。
    你同时还是一个人才分析专家，根据用户给定的论文链接，依照岗位tag文档，判定论文作者符合哪两个岗位描述，以json格式返回适合的岗位tag以及对应负责人。
    同样的，还请你先判断以下论文作者中是否有华人，并以json格式返回“是”或“否”。
    json: score: int, summary: str, tag_primary: str, contact_tag_primary: str, tag_secondary: str, contact_tag_secondary, 是否有华人: str
    论文评阅 SOP 如下： {sop_content}
    岗位tag文档如下: {tag_content}"""

    if is_pdf:
        return [
            {"role": "system", "content": f"{system_prompt}"},
        {"role": "user", "content": f"论文内容：{paper_content}"},
        ]
    else:
        return [
            {"role": "system", "content": f"{system_prompt}"},
            {"role": "user", "content": f"论文链接：{paper_content}"},
        ]

#对于论文作者中没有华人的文章，则不需要依照岗位tag文档进行岗位符合度的判断。

# SDK 使用说明: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/preparations-before-development
# 以下示例代码默认根据文档示例值填充，如果存在代码问题，请在 API 调试台填上相关必要参数后再复制代码使用
def add_records_to_dowei(
    table_app_token: str,
    table_id: str,
    user_access_token: str,
    records: List[Dict[str, Any]],
):
    # 创建client
    # 使用 user_access_token 需开启 token 配置, 并在 request_option 中配置 token
    client = (
        lark.Client.builder()
        .enable_set_token(True)
        .log_level(lark.LogLevel.DEBUG)
        .build()
    )

    # 构造请求对象
    request: BatchCreateAppTableRecordRequest = (
        BatchCreateAppTableRecordRequest.builder()
        .app_token(table_app_token)
        .table_id(table_id)
        .request_body(
            BatchCreateAppTableRecordRequestBody.builder()
            .records(
                [
                    AppTableRecord().builder().fields(record).build()
                    for record in records
                ]
            )
            .build()
        )
        .build()
    )

    # 发起请求
    option = lark.RequestOption.builder().user_access_token(user_access_token).build()
    response: BatchCreateAppTableRecordResponse = (
        client.bitable.v1.app_table_record.batch_create(request, option)
    )

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.bitable.v1.app_table_record.batch_create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
        )
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))

def get_feishu_sheet_content(doc_token: str, sheet_id: str, range: str, access_token: str) -> list[str]:
    """
    通过飞书开放平台 API 获取电子表格的内容
    Reference: https://open.larkoffice.com/document/server-docs/docs/sheets-v3/data-operation/reading-a-single-range
    
    Args:
        doc_token: 电子表格的 token
        sheet_id: 工作表 ID
        range: 单元格范围，如 "A1:B2"
        access_token: 访问令牌
    
    Returns:
        表格内容，格式为列表，例如 [1,2,3]
    """
    # 构建请求 URL
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{doc_token}/values/{sheet_id}!{range}"
    
    # 设置请求头
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        # 发送 GET 请求
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 检查请求是否成功
        
        # 解析 JSON 响应
        data = response.json()
        
        # 提取 values 部分
        values = data.get("data", {}).get("valueRange", {}).get("values", [])
        return values
    
    except requests.exceptions.RequestException as e:
        print(f"请求出错: {e}")
        return []
    except (KeyError, ValueError) as e:
        print(f"解析响应出错: {e}")
        return []

def add_records_to_feishu_sheet(spreadsheet_token, sheet_id, range, user_access_token, results):
    """
    向飞书表格指定范围写入数据
    
    Args:
        spreadsheet_token: 电子表格的 token
        sheet_id: 工作表 ID
        range: 写入范围（如 "A1:B5"）
        results: 要写入的数据（二维列表，如 [[1, "a"], [2, "b"]]）
    
    Returns:
        dict: API 响应结果，包含操作状态和相关数据
    """
    # 飞书 API 基础配置
    base_url = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets"
    access_token = user_access_token # 需替换为实际令牌
    
    # 构建请求 URL（包含完整范围）
    url = f"{base_url}/{spreadsheet_token}/values"
    
    # 设置请求头
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # 构建请求体
    payload = {
        "valueRange": {
            "range": f"{sheet_id}!{range}",
            "values": results
        }
    }
    
    try:
        # 发送 PUT 请求
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # 检查请求是否成功
        
        # 返回 API 响应
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"请求发送失败: {e}")
        return {"error": str(e)}
    except json.JSONDecodeError as e:
        print(f"响应解析失败: {e}")
        return {"error": f"响应解析失败: {response.text}"}


# 清理链接（去除锚点和查询参数）
def clean_link(link):
        # 移除#后面的部分（如#community）
        link = re.sub(r'#.*$', '', link)
        # 移除?后面的查询参数
        link = re.sub(r'\?.*$', '', link)
        return link

def get_huggingface_daily_papers_arxiv_links(date_str=None) -> tuple[list[str], str]:
    """
    从Hugging Face Daily Papers获取arXiv链接，自动去重并返回列表
    
    Args:
        date_str (str, optional): 指定日期(YYYY-MM-DD)，默认为上一个工作日
    
    Returns:
        list: 去重后的arXiv链接列表
        str: 论文对应的发表日期
    """
    # 计算日期（默认为上一个工作日）
    if not date_str:
        today = datetime.today()
        offset = 1
        while True:
            last_working_day = today - timedelta(days=offset)
            if last_working_day.weekday() < 5:  # 0-4是工作日
                break
            offset += 1
        date_str = last_working_day.strftime("%Y-%m-%d")
    
    url = f"https://huggingface.co/papers/date/{date_str}"
    print(f"正在获取{date_str}的Daily Papers: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # 发送HTTP请求
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # 解析HTML内容
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取arXiv链接
        arxiv_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/papers/' in href:
                parts = href.split('/')
                arxiv_id = next((p for p in parts if re.match(r'\d+\.\d+', p)), None)
                if arxiv_id:
                    arxiv_links.append(f"https://arxiv.org/abs/{arxiv_id}")
        
        cleaned_links = [clean_link(link) for link in arxiv_links]
        
        # 内存去重（去除本次爬取中的重复链接）
        unique_links = list(set(cleaned_links))
        unique_links.sort()  # 排序方便查看
    
        print(f"成功获取{len(unique_links)}个唯一Hugging Face链接")
        return unique_links, date_str
        
    except requests.exceptions.RequestException as e:
        print(f"请求出错: {e}")
        return []
    except Exception as e:
        print(f"发生错误: {e}")
        return []

# 简单测试（可在其他程序中导入时忽略）
#if __name__ == "__main__":
#    links = get_huggingface_daily_papers_arxiv_links()
#    if links:
#       print(f"前3个链接示例：\n{links}")



def get_arxiv_paper_links(date_str: str = None) -> tuple[list[str], str]:
    """
    爬取UTC时区前一个工作日 arXiv 上 AI 领域的所有论文 PDF 链接
    
    Args:
        date_str (str, optional): 指定日期(YYYY-MM-DD)，默认为上一个工作日

    Returns:
        list[str]: 去重后的pdf links
        str: 论文对应的提交日期
    """
    
    # 计算日期（默认为上一个工作日）
    if not date_str:
        today = datetime.now()
        offset = 1
        while True:
            last_working_day = today - timedelta(days=offset)
            if last_working_day.weekday() < 5:  # 0-4是工作日
                break
            offset += 1
        date_str = last_working_day.strftime("%Y-%m-%d")
        next_date = last_working_day + timedelta(days = 1)
        next_date_str = next_date.strftime("%Y%m%d")
    else:
        date_obj = datetime.strptime(date_str,"%Y-%m-%d")
        next_date = date_obj + timedelta(days=1)
        next_date_str = next_date.strftime("%Y%m%d")        
    
    # 构建查询
    query = (
        "cat:cs.AI "
        f"AND submittedDate:[{date_str.replace('-','')}000000 TO {next_date_str}000000]"
    )
    
    # 执行查询
    search = arxiv.Search(
        query=query,
        max_results=10000,  # 假设一天最多10000篇论文，可根据需要调整
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending
    )
    
    # 创建client
    client = arxiv.Client()

    # 提取并返回 PDF 链接
    pdf_links = [result.pdf_url for result in client.results(search)]
    
    #清理PDF链接
    pdf_links = [clean_link(link) for link in pdf_links]
    print(f"成功获取UTC时间{date_str}的{len(pdf_links)}个唯一arXiv链接")

    return pdf_links, date_str