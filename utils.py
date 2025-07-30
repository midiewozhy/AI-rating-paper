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
            lark.logger.info(result)

            # 返回包含访问令牌和过期时间的字典
            return {
                "access_token": access_token,
                "expire": expire,
                "timestamp": int(time.time()),  # 添加获取时间戳
            }
        else:
            # 打印错误信息
            lark.logger.error(
                f"获取access_token失败，错误码：{result.get('code')}，错误信息：{result.get('msg')}"
            )
            return None
    except requests.exceptions.RequestException as e:
        # 处理请求异常
        lark.logger.error(f"请求异常：{e}")
        return None
    except json.JSONDecodeError as e:
        # 处理响应解析异常
        lark.logger.error(f"响应解析异常：{e}")
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


def get_rating_prompt(sop_content: str, tag_content: str, paper_content: str, relevance_content: str = None) -> list:

    system_prompt = f"""
    你是一个专业的评阅人兼人才分析专家。请根据用户提供的论文链接，结合给定的文档信息，严格按以下逻辑执行任务，并最终输出指定的JSON格式。

    处理逻辑：
    1. 论文总结与评分：
    - 对论文进行总结
    - 依据论文评阅SOP文档为论文打整数分数
    - {sop_content}

    2. 人才岗位匹配分析：
    - 依据岗位tag文档分析作者符合的两个岗位
    - {tag_content}
    - 按相关性由高到低排序确定主要和次要岗位
    - 提取对应的负责人信息

    3. 华人作者判断：
    - 分析论文作者名单判断是否有华人作者
    - 依据：作者姓名（常见华人姓氏、拼音）、所属机构（中国大陆/港澳台/新加坡等）

    输出要求：
    - 仅输出一个**可直接被JSON解析器解析**的对象，使用```json和```包裹。
    - 严格遵循以下结构（包括字段顺序、引号、逗号等），示例：
    ```json
    {{
    "score": 67,
    "summary": "论文提出了RICE方法...（总结需包含优缺点、打分原因、岗位匹配原因，注意转义双引号和换行）",
    "tag_primary": "多模态交互与世界模型-VLM基础模型",
    "contact_tag_primary": "林毅、吴侑彬、秦晓波",
    "tag_secondary": "视觉-视觉模型工程",
    "contact_tag_secondary": "xuefeng xiao、rui wang",
    "是否有华人": "是"
    }}

    关键规则：
    - 所有判断必须严格基于两个文档内容
    - 是否有华人字段必须为"是"或"否"
    """

    #，否则固定为'与公司业务无相同点'
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
        lark.logger.error(f"请求出错: {e}")
        return []
    except (KeyError, ValueError) as e:
        lark.logger.error(f"解析响应出错: {e}")
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
        lark.logger.error(f"请求发送失败: {e}")
        return {"error": str(e)}
    except json.JSONDecodeError as e:
        lark.logger.error(f"响应解析失败: {e}")
        return {"error": f"响应解析失败: {response.text}"}


# 清理链接（去除锚点和查询参数）
def clean_link(link):
        # 移除#后面的部分（如#community）
        link = re.sub(r'#.*$', '', link)
        # 移除?后面的查询参数
        link = re.sub(r'\?.*$', '', link)
        return link

def get_huggingface_daily_papers_arxiv_links(date_str=None):
    """
    从Hugging Face Daily Papers获取arXiv链接，自动去重并返回列表
    
    Args:
        date_str (str, optional): 指定日期(YYYY-MM-DD)，默认为上一个工作日
    
    Returns:
        list: 去重后的arXiv链接列表
        str: 论文对应的发表日期
    """

    # 创建一个用来去重的存储器
    hf_visited = set()
    hf_count = 0

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
    lark.logger.info(f"正在获取{date_str}的Daily Papers: {url}")
    
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
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if '/papers/' in href:
                parts = href.split('/')
                arxiv_id = next((p for p in parts if re.match(r'\d+\.\d+', p)), None)
                if arxiv_id and clean_link(arxiv_id) not in hf_visited:
                    hf_visited.add(arxiv_id)    
                    hf_count += 1                
                    yield (f"https://arxiv.org/pdf/{arxiv_id}",1, date_str)
        
        lark.logger.info(f"成功爬取到Hugging Face上{date_str}的{hf_count}条链接")

    except requests.exceptions.RequestException as e:
        lark.logger.error(f"请求出错: {e}，已经成功爬取到{hf_count}条链接")
    except Exception as e:
        lark.logger.error(f"发生错误: {e}，已经成功爬取到{hf_count}条链接")




def get_arxiv_paper_links():
    """
    爬取前一个公布周期arXiv上AI领域的所有论文PDF链接

    Returns:
        list[str]: 去重后的pdf links
        str: 论文对应的提交周期
    """
    
    # 创建一个用来去重的存储器
    arxiv_visited = set()
    # 计数
    arxiv_count = 0

    #计算日期
    today = datetime.today()
    if today.weekday() in (0,1):
        start_date = today - timedelta(days = 4) # 周一从上周四开始爬，周二从上周五开始爬
    else:
        start_date = today - timedelta(days = 2) # 周三/四/五都从当天的前天开始爬
    if today.weekday() == 0:
        end_date = today - timedelta(days = 3) # 周一爬到上周五
    else:
        end_date = today - timedelta(days = 1) # 其他都爬到前一天

    # 转换成字符串
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")
    period_str = f"EDT {start_date_str} 14:00到{end_date_str} 14:00"

    # 构建query
    query = f'cat:cs.AI AND submittedDate:[{start_date_str}1400 TO {end_date_str}1400]'
    
    # 执行查询
    search = arxiv.Search(
        query=query,
        max_results=10000,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending
    )

    # 生成链接
    try:
        for result in arxiv.Client().results(search):
            link = clean_link(result.pdf_url)
            if link not in arxiv_visited and arxiv_visited.add(link) is None:
                arxiv_count += 1
                yield (link, 0, period_str)
        lark.logger.info(f"成功爬取到ArXiv上{period_str}的{arxiv_count}条链接")        

    except Exception as e:
        lark.logger.error(f"爬取arxiv链接时出错: {e}，已经成功爬取到{arxiv_count}条链接")
        raise

