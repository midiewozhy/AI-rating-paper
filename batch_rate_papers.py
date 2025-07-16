import threading
import queue
import json
from importlib import reload
from typing import Any, Dict, List

import constants
import utils

reload(constants)
reload(utils)
import json
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

# 初始化一个全局client
client = OpenAI(
    base_url="https://ark.cn-beijing.volces.com/api/v3/bots",
    api_key=ARK_API_KEY,
)


def rate_papers(sop_content: str, tag_content: str, date_str: str, link: str, relevance_content: str = None) -> Optional[dict[str, any]]:
    """对单篇论文进行评分

    Args:
        sop_content (str): 评分标准内容
        tag_content (str): 岗位tag内容
        link (str): 论文链接
        relevance_content (str): 研究相关性内容

    Returns:
        dict[str, any]: 对应链接的评分结果
    """

    result = {}

    #处理pdf url链接输入
    if link != '':    
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
                return None

            # 解析评分结果
            ai_ret = completion.choices[0].message.content.strip()
            lark.logger.info(f"ai_ret: {ai_ret}")
            ai_ret = re.sub(r'^(<\|FunctionCallEnd\|>|```json\n?|```\n?)', '', ai_ret, flags=re.IGNORECASE)
            ai_ret = re.sub(r'```\s*$', '', ai_ret)  # 移除结尾的代码块标记

            #检查是否为空内容
            if not ai_ret:
                lark.logger.error(f"清理后内容为空，跳过论文: {link}")
                return None
            
            #添加所需字段
            result = json.loads(ai_ret)
            result["link"] = {"link": link, "text": link}
            result["date"] = date_str

        except json.JSONDecodeError as e:
            lark.logger.error(f"解析JSON出错：{e}，内容：{ai_ret}，跳过论文: {link}")
        except Exception as e:
            lark.logger.error(f"处理论文时发生意外错误：{type(e).__name__} - {str(e)}，跳过论文: {link}")

        return result
    else:
        lark.logger.error("链接为空，跳过")
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

    # 初始化任务队列
    task_queue = queue.Queue()
    arxiv_results = []
    hf_results = []
    producer_done_lock = threading.Lock()
    producer_count = 2  # 总生产者数量
    producer_done_count = 0  # 已完成的生产者数量
    result_lock = threading.Lock() # 保证消费线程的安全性
    
    # 定义生产者
    def arxiv_producer():
        nonlocal producer_done_count
        try:
            # 遍历arxiv生成器 （yield (link, 0, date)）
            for item in get_arxiv_paper_links():
                task_queue.put(item)  # 实时入队
                lark.logger.info(f"Arxiv爬取到链接并入队: {item[0]}（date: {item[2]}）")
            lark.logger.info("Arxiv爬取完成，所有链接已入队")
        except Exception as e:
            lark.logger.error(f"Arxiv爬取线程出错: {e}")
        finally:
            # 最后一个生产者完成时，标记producer_done为True
            with producer_done_lock:
                producer_done_count += 1

    def hf_producer():
        """hf爬取生产者：实时将(link, 1, date)放入队列"""
        nonlocal producer_done_count
        try:
            # 遍历hf生成器（假设已改为yield (link, 1, date)）
            for item in get_huggingface_daily_papers_arxiv_links():
                task_queue.put(item)  # 实时入队
                lark.logger.info(f"Hugging Face爬取到链接并入队: {item[0]}（date: {item[2]}）")
            lark.logger.info("Hugging Face爬取完成，所有链接已入队")
        except Exception as e:
            lark.logger.error(f"Hugging Face爬取线程出错: {e}")
        finally:
            # 最后一个生产者完成时，标记producer_done为True
            with producer_done_lock:
                producer_done_count += 1

    # 定义消费者
    def consumer():
        while True:
            try:
                # 从队列取任务（超时5秒，避免无限阻塞）
                item = task_queue.get(timeout=5)
                # 解析三元组：(link, tag, date)
                link, tag, date = item
                lark.logger.info(f"消费者处理链接（tag={tag}）: {link}")

            # 调用大模型评分（复用rate_papers，传入单链接）
                # 注意：rate_papers已改为使用全局OpenAI客户端
                rating_result = rate_papers(
                    sop_content=sop_content,
                    tag_content=tag_content,
                    date_str=date,
                    link=link,  # 单链接作为列表传入
                )

                # 按tag保存结果（线程安全）
                if rating_result:
                    with result_lock:
                        if tag == 0:
                            arxiv_results.append(rating_result)
                        elif tag == 1:
                            hf_results.append(rating_result)

                # 标记任务完成
                task_queue.task_done()
                lark.logger.info(f"消费者完成链接（tag={tag}）: {link}")

            except queue.Empty:
                # 队列为空时，检查是否所有生产者已完成
                with producer_done_lock:
                    if producer_done_count == producer_count and task_queue.empty():
                        lark.logger.info("消费者：所有任务已处理，退出")
                        break  # 退出循环
            except Exception as e:
                lark.logger.error(f"消费者处理出错: {e}")
                if 'item' in locals():
                    task_queue.task_done()  # 确保任务计数正确


     # -------------------------- 核心运行逻辑 --------------------------
    # 1. 启动生产者线程
    arxiv_thread = threading.Thread(target=arxiv_producer, name="arxiv-producer")
    hf_thread = threading.Thread(target=hf_producer, name="hf-producer")
    arxiv_thread.start()
    hf_thread.start()
    lark.logger.info("所有生产者线程已启动")

    # 2. 启动消费者线程（20个并发处理）
    consumer_count = 20
    consumer_threads = []
    for i in range(consumer_count):
        t = threading.Thread(target=consumer, name=f"consumer-{i}")
        t.start()
        consumer_threads.append(t)
    lark.logger.info(f"已启动{consumer_count}个消费者线程")

    # 3. 等待生产者线程完成（确保所有链接入队）
    arxiv_thread.join()
    hf_thread.join()
    lark.logger.info("所有生产者线程已完成爬取")

    # 4. 等待队列中所有任务处理完毕
    task_queue.join()
    lark.logger.info("队列中所有论文链接已处理完毕")

    # 5. 等待所有消费者线程退出
    for t in consumer_threads:
        t.join()
    lark.logger.info("所有消费者线程已退出")

    # 6. 最终结果处理（保存到飞书/本地等）
    lark.logger.info(f"处理完成：arxiv共{len(arxiv_results)}篇，hf共{len(hf_results)}篇")

    # 保存arxiv结果到飞书多维表格（假设函数已定义）
    if arxiv_results:
        arxiv_results.append(dict())
        save_to_feishu_duowei(arxiv_results, ARXIV_TABLE_ID)
        lark.logger.info(f"已保存{len(arxiv_results)-1}条arxiv结果到飞书")
    else:
        lark.logger.warning("未获取到有效arxiv结果")

    # 保存hf结果到飞书多维表格
    if hf_results:
        hf_results.append(dict())
        save_to_feishu_duowei(hf_results, HUGGING_FACE_TABLE_ID)
        lark.logger.info(f"已保存{len(hf_results)-1}条hf结果到飞书")
    else:
        lark.logger.warning("未获取到有效hf结果")

    lark.logger.info("整个论文处理流程已完成") 



if __name__ == "__main__":
    main()

