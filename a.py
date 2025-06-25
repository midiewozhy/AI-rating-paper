import arxiv
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re

# 清理链接（去除锚点和查询参数）
def clean_link(link):
        # 移除#后面的部分（如#community）
        link = re.sub(r'#.*$', '', link)
        # 移除?后面的查询参数
        link = re.sub(r'\?.*$', '', link)
        return link


def get_arxiv_paper_links(date_str: str = None) -> list[str]:
    """
    爬取指定日期 arXiv 上 AI 领域的所有论文 PDF 链接
    
    Args:
        str: 指定日期，默认上一个工作日
    Returns:
        list: 去重后的pdf links
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
        date_str = last_working_day.strftime("%Y%m%d")
        next_date = last_working_day + timedelta(days = 1)
        next_date_str = next_date.strftime("%Y%m%d")
    else:
        date_obj = datetime.strptime(date_str, "%Y%m%d").date()
        next_date = date_obj + timedelta(days=1)
        next_date_str = next_date.strftime("%Y%m%d")
    
    # 构建查询
    query = (
        "cat:cs.AI "
        f"AND submittedDate:[{date_str}0000 TO {next_date_str}0000]"
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
    print(f"成功获取{len(pdf_links)}个唯一arXiv链接")

    return pdf_links


a = get_arxiv_paper_links()
print(a)