"""
千问大模型专属通讯模块 (LLM Fallback Service)
当前两关（精确数据库查找A，向量相似查找B）全部失败，什么都没搜到或者匹配度不够时，
这个模块就会扮演最后的兜底角色法宝（C），把悬念交给阿里千问的推理大脑完成智能推理！
"""
import os
import json
from core.config import get_logger

logger = get_logger(__name__)

def evaluate_mapping_via_llm(src_field: str, src_desc: str) -> dict:
    """
    封装了阿里千问 Qwen 语言大模型（类似ChatGPT）的调用逻辑。
    把您未知的物料/字段等业务术语发给大模型，让大模型猜一个对应的 SAP 标准词！
    """
    import dashscope
    # 您必须在环境变量中存好您的 API 密钥，有了大门的钥匙它才能允许您调动模型能力
    dashscope.api_key = os.environ.get("DASHSCOPE_API_KEY")
    
    if not dashscope.api_key:
        logger.warning("没有找到千问的大模型密钥(DASHSCOPE_API_KEY)，自动跳过C环节大模型求助。")
        return {}

    # ======= 核心 Prompt：人设与约束！ =======
    # 这里非常重要，告诉 AI 他是 SAP 专家，必须强硬地返回带中括号和大括号的 JSON 机器格式，千万别跟我闲聊说空话。
    sys_prompt = "You are an SAP mapping expert. A user provides a Source System Field name and Description. Predict the corresponding SAP Table Name, SAP Field Name, and SAP Field Description. Return ONLY a valid JSON object with keys: 'sap_table_name', 'sap_field_name', 'sap_field_desc'. The 'sap_field_desc' MUST be written in the exact same language as the provided Source Description. If you absolutely do not know the mappings, return an empty string for all three."
    
    # 包装好给模型提的问题
    user_prompt = f"Source Field: {src_field}, Description: {src_desc}"

    try:
        # 发起 HTTP 在线握手请求，采用 qwen-plus 智力中坚模型
        response = dashscope.Generation.call(
            model='qwen-plus',
            messages=[
                {'role': 'system', 'content': sys_prompt},
                {'role': 'user', 'content': user_prompt}
            ],
            result_format='message'
        )

        # 200 就是成功应答暗语
        if response.status_code == 200:
            content = response.output.choices[0]['message']['content']
            
            # 如果大模型自作聪明加上了 ```json 的代码块外壳，这里用切片法给它强行扒得只剩内核。
            if "```json" in content:
                content = content.split("```json")[-1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[-1].split("```")[0].strip()
                
            # 把字符串转换为 Python 可控的安全字典对象
            pred = json.loads(content)
            
            # 返回干净的三大 SAP 映射件 (表、技术名、项目名) 供后段填表
            return {
                'sap_table_name': pred.get('sap_table_name', ''),
                'sap_field_name': pred.get('sap_field_name', ''),
                'sap_field_desc': pred.get('sap_field_desc', '')
            }
        else:
            logger.error(f"大模型 API 求助失败: {response.code} {response.message}")
            return {}
            
    except Exception as llm_e:
        # 这个为了防崩溃，网络断了等突发情况就不会导致您的表格跑到一半废了
        logger.warning(f"大模型兜底由于异常已放弃 '{src_field}': {llm_e}")
        return {}
